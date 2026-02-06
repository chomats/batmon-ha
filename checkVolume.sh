#!/bin/bash

# Script pour analyser un volume Docker
# Usage: ./checkVolume.sh <volume-name>

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

VOLUME_NAME="$1"

if [ -z "$VOLUME_NAME" ]; then
    echo -e "${RED}Usage: $0 <volume-name>${NC}"
    echo ""
    echo "Exemples:"
    echo "  $0 grafana-storage"
    echo "  $0 prometheus_data"
    echo ""
    echo "Pour lister tous les volumes:"
    echo "  docker volume ls"
    exit 1
fi

echo -e "${CYAN}==================================${NC}"
echo -e "${CYAN}Analyse du volume: ${YELLOW}$VOLUME_NAME${NC}"
echo -e "${CYAN}==================================${NC}"
echo ""

# 1. Vérifier si le volume existe
if ! docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
    echo -e "${RED}❌ Le volume '$VOLUME_NAME' n'existe pas${NC}"
    echo ""
    echo -e "${YELLOW}Volumes disponibles:${NC}"
    docker volume ls
    exit 1
fi

echo -e "${GREEN}✅ Volume trouvé${NC}"
echo ""

# 2. Point de montage sur l'hôte
MOUNTPOINT=$(docker volume inspect "$VOLUME_NAME" --format '{{.Mountpoint}}')
echo -e "${BLUE}📁 Point de montage hôte:${NC}"
echo -e "   ${YELLOW}$MOUNTPOINT${NC}"
echo ""

# 3. Driver et options
DRIVER=$(docker volume inspect "$VOLUME_NAME" --format '{{.Driver}}')
echo -e "${BLUE}🔧 Driver:${NC} $DRIVER"
echo ""

# 4. Taille du volume
echo -e "${BLUE}💾 Taille:${NC}"
if [ -d "$MOUNTPOINT" ]; then
    SIZE=$(sudo du -sh "$MOUNTPOINT" 2>/dev/null | cut -f1)
    if [ -n "$SIZE" ]; then
        echo -e "   ${GREEN}$SIZE${NC}"
    else
        SIZE_NO_SUDO=$(du -sh "$MOUNTPOINT" 2>/dev/null | cut -f1)
        if [ -n "$SIZE_NO_SUDO" ]; then
            echo -e "   ${GREEN}$SIZE_NO_SUDO${NC}"
        else
            echo -e "   ${YELLOW}⚠️  Impossible d'accéder (essayez avec sudo)${NC}"
        fi
    fi
else
    echo -e "   ${YELLOW}⚠️  Point de montage inaccessible${NC}"
fi
echo ""

# 5. Conteneurs utilisant ce volume
echo -e "${BLUE}🐳 Conteneurs utilisant ce volume:${NC}"
FOUND=false
docker ps -a --format '{{.Names}}' | while read container; do
    if docker inspect "$container" 2>/dev/null | grep -q "\"Name\": \"$VOLUME_NAME\""; then
        FOUND=true
        STATUS=$(docker inspect "$container" --format '{{.State.Status}}')
        MOUNT_PATH=$(docker inspect "$container" | jq -r --arg vol "$VOLUME_NAME" '.[].Mounts[] | select(.Name == $vol) | .Destination' 2>/dev/null)
        
        # Couleur selon le statut
        case $STATUS in
            running)
                STATUS_COLOR="${GREEN}"
                ;;
            exited)
                STATUS_COLOR="${YELLOW}"
                ;;
            *)
                STATUS_COLOR="${RED}"
                ;;
        esac
        
        echo -e "   ${GREEN}✓${NC} ${CYAN}$container${NC} (${STATUS_COLOR}$STATUS${NC}) → ${YELLOW}$MOUNT_PATH${NC}"
    fi
done

# Si aucun conteneur trouvé
if ! $FOUND; then
    # Vérifier avec une autre méthode
    CONTAINERS=$(docker ps -a --filter volume="$VOLUME_NAME" --format '{{.Names}}')
    if [ -z "$CONTAINERS" ]; then
        echo -e "   ${YELLOW}⚠️  Aucun conteneur n'utilise ce volume (volume orphelin)${NC}"
    else
        echo "$CONTAINERS" | while read container; do
            STATUS=$(docker inspect "$container" --format '{{.State.Status}}' 2>/dev/null)
            echo -e "   ${GREEN}✓${NC} ${CYAN}$container${NC} (${STATUS})"
        done
    fi
fi
echo ""

# 6. Labels
echo -e "${BLUE}🏷️  Labels:${NC}"
LABELS=$(docker volume inspect "$VOLUME_NAME" --format '{{json .Labels}}')
if [ "$LABELS" = "null" ] || [ "$LABELS" = "{}" ]; then
    echo -e "   ${YELLOW}Aucun label${NC}"
else
    echo "$LABELS" | jq -r 'to_entries[] | "   \(.key) = \(.value)"' 2>/dev/null || echo -e "   ${YELLOW}Impossible de parser les labels${NC}"
fi
echo ""

# 7. Date de création
CREATED=$(docker volume inspect "$VOLUME_NAME" --format '{{.CreatedAt}}')
echo -e "${BLUE}📅 Créé le:${NC} $CREATED"
echo ""

# 8. Contenu du volume (aperçu)
echo -e "${BLUE}📄 Contenu (aperçu - 10 premiers fichiers):${NC}"
if [ -d "$MOUNTPOINT" ]; then
    if sudo ls -lah "$MOUNTPOINT" 2>/dev/null | head -11 | tail -10; then
        :
    else
        if ls -lah "$MOUNTPOINT" 2>/dev/null | head -11 | tail -10; then
            :
        else
            echo -e "   ${YELLOW}⚠️  Impossible de lister le contenu (permissions)${NC}"
        fi
    fi
else
    echo -e "   ${YELLOW}⚠️  Point de montage inaccessible${NC}"
fi
echo ""

echo -e "${CYAN}==================================${NC}"
echo -e "${GREEN}Commandes utiles:${NC}"
echo ""
echo -e "  ${BLUE}# Supprimer ce volume${NC}"
echo -e "  docker volume rm $VOLUME_NAME"
echo ""
echo -e "  ${BLUE}# Sauvegarder ce volume${NC}"
echo -e "  docker run --rm -v $VOLUME_NAME:/source -v \$(pwd):/backup alpine tar czf /backup/${VOLUME_NAME}-backup.tar.gz -C /source ."
echo ""
echo -e "  ${BLUE}# Restaurer ce volume${NC}"
echo -e "  docker run --rm -v $VOLUME_NAME:/target -v \$(pwd):/backup alpine tar xzf /backup/${VOLUME_NAME}-backup.tar.gz -C /target"
echo ""
echo -e "${CYAN}==================================${NC}"
