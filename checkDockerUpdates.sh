#!/bin/bash

# Script pour vérifier les mises à jour des images Docker dans compose.yml
# Usage: ./checkDockerUpdates.sh [fichier-compose.yml]

set -e

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fichier compose par défaut
COMPOSE_FILE="${1:-doc/compose.yml}"

# Vérifier si le fichier existe
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}Erreur: Le fichier $COMPOSE_FILE n'existe pas!${NC}"
    exit 1
fi

echo -e "${BLUE}==================================${NC}"
echo -e "${BLUE}Vérification des mises à jour${NC}"
echo -e "${BLUE}Fichier: ${YELLOW}$COMPOSE_FILE${NC}"
echo -e "${BLUE}==================================${NC}"
echo ""

# Extraire les images du fichier compose.yml (exclut les lignes commentées)
IMAGES=$(grep -E "^\s*image:" "$COMPOSE_FILE" | grep -v "^\s*#" | awk '{print $2}' | sort -u)

if [ -z "$IMAGES" ]; then
    echo -e "${YELLOW}Aucune image trouvée dans $COMPOSE_FILE${NC}"
    exit 0
fi

echo -e "${GREEN}Images trouvées:${NC}"
echo "$IMAGES" | while read -r image; do
    echo -e "  ${YELLOW}→${NC} $image"
done
echo ""

# Fonction pour obtenir le digest d'une image
get_remote_digest() {
    local image=$1
    docker manifest inspect "$image" 2>/dev/null | grep -m1 '"digest"' | awk '{print $2}' | tr -d ',"'
}

get_local_digest() {
    local image=$1
    docker inspect --format='{{.RepoDigests}}' "$image" 2>/dev/null | grep -o 'sha256:[a-f0-9]*' | head -n1
}

# Variable pour compter les mises à jour
UPDATES_AVAILABLE=0
UPDATES_NEEDED=()

echo -e "${BLUE}Vérification des mises à jour...${NC}"
echo ""

# Vérifier chaque image
echo "$IMAGES" | while read -r image; do
    if [ -z "$image" ]; then
        continue
    fi
    
    echo -e "${YELLOW}Vérification:${NC} $image"
    
    # Vérifier si l'image existe localement
    if docker inspect "$image" >/dev/null 2>&1; then
        LOCAL_DIGEST=$(get_local_digest "$image")
        echo -e "  ${BLUE}Local digest:${NC}  $LOCAL_DIGEST"
    else
        LOCAL_DIGEST=""
        echo -e "  ${YELLOW}Image non présente localement${NC}"
    fi
    
    # Pull les métadonnées de l'image distante (sans télécharger l'image)
    echo -e "  ${BLUE}Récupération des métadonnées distantes...${NC}"
    REMOTE_DIGEST=$(get_remote_digest "$image")
    
    if [ -z "$REMOTE_DIGEST" ]; then
        echo -e "  ${RED}✗ Impossible de récupérer les informations distantes${NC}"
        echo ""
        continue
    fi
    
    echo -e "  ${BLUE}Remote digest:${NC} $REMOTE_DIGEST"
    
    # Comparer les digests
    if [ -z "$LOCAL_DIGEST" ]; then
        echo -e "  ${GREEN}⚠ Image à télécharger${NC}"
        UPDATES_AVAILABLE=$((UPDATES_AVAILABLE + 1))
        UPDATES_NEEDED+=("$image")
    elif [ "$LOCAL_DIGEST" != "$REMOTE_DIGEST" ]; then
        echo -e "  ${GREEN}✓ Mise à jour disponible!${NC}"
        UPDATES_AVAILABLE=$((UPDATES_AVAILABLE + 1))
        UPDATES_NEEDED+=("$image")
    else
        echo -e "  ${GREEN}✓ Image à jour${NC}"
    fi
    
    echo ""
done

# Résumé final
echo -e "${BLUE}==================================${NC}"
if [ $UPDATES_AVAILABLE -eq 0 ]; then
    echo -e "${GREEN}✓ Toutes les images sont à jour!${NC}"
else
    echo -e "${YELLOW}⚠ $UPDATES_AVAILABLE image(s) nécessite(nt) une mise à jour${NC}"
    echo ""
    echo -e "${YELLOW}Pour mettre à jour:${NC}"
    echo -e "  ${BLUE}# Méthode 1: Docker Compose${NC}"
    echo -e "  cd $(dirname $COMPOSE_FILE)"
    echo -e "  docker compose pull"
    echo -e "  docker compose up -d"
    echo ""
    echo -e "  ${BLUE}# Méthode 2: Manuellement${NC}"
    for img in "${UPDATES_NEEDED[@]}"; do
        echo -e "  docker pull $img"
    done
fi
echo -e "${BLUE}==================================${NC}"

exit $UPDATES_AVAILABLE
