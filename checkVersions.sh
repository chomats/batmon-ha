#!/bin/bash

# Script pour vérifier les versions d'images Docker et détecter les changements
# Usage: ./checkVersions.sh [image-name]

set -e

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Fonction d'aide
show_help() {
    echo "Usage: $0 [options] [image]"
    echo ""
    echo "Options:"
    echo "  -f, --file FILE    Fichier compose.yml à analyser"
    echo "  -a, --all          Vérifier toutes les images dans compose.yml"
    echo "  -h, --help         Afficher cette aide"
    echo ""
    echo "Exemples:"
    echo "  $0 homeassistant/aarch64-homeassistant"
    echo "  $0 ghcr.io/fl4p/batmon-ha"
    echo "  $0 -f doc/compose.yml -a"
    exit 0
}

# Fonction pour extraire la version depuis le tag
extract_version_from_tag() {
    local tag=$1
    # Supprime 'latest', 'stable', etc. et extrait les numéros de version
    echo "$tag" | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1
}

# Fonction pour récupérer les tags disponibles d'une image
get_available_tags() {
    local image=$1
    local repo=""
    local image_name=""
    
    # Parser l'image (registry/repo ou juste repo)
    if [[ "$image" == *"/"* ]]; then
        repo=$(echo "$image" | cut -d':' -f1)
        image_name=$(basename "$repo")
    else
        repo="$image"
        image_name="$image"
    fi
    
    echo -e "${BLUE}Récupération des tags pour:${NC} $repo"
    
    # Utiliser l'API Docker Hub ou registry
    if [[ "$repo" == ghcr.io/* ]]; then
        # GitHub Container Registry
        local package_name=$(echo "$repo" | sed 's|ghcr.io/||')
        echo -e "${YELLOW}  Source: GitHub Container Registry${NC}"
        # Note: GHCR nécessite authentification pour l'API
        echo -e "${YELLOW}  Utilisez: docker pull $repo:<tag> pour voir les versions${NC}"
        return
    elif [[ "$repo" == *"/"* ]] && [[ "$repo" != *"."* ]]; then
        # Docker Hub (format: user/image)
        local namespace=$(echo "$repo" | cut -d'/' -f1)
        local repository=$(echo "$repo" | cut -d'/' -f2)
        echo -e "${YELLOW}  Source: Docker Hub${NC}"
        
        # API Docker Hub v2
        local url="https://hub.docker.com/v2/repositories/${namespace}/${repository}/tags?page_size=25&ordering=last_updated"
        local response=$(curl -s "$url")
        
        if echo "$response" | jq -e '.results' >/dev/null 2>&1; then
            echo "$response" | jq -r '.results[] | "\(.name)\t\(.last_updated)"' | head -10
        else
            echo -e "${RED}  Erreur lors de la récupération des tags${NC}"
        fi
    else
        # Registry personnalisé ou format non standard
        echo -e "${YELLOW}  Registry personnalisé détecté${NC}"
        docker pull "$repo" --all-tags 2>&1 | grep "Pulling from" || true
    fi
}

# Fonction principale de vérification de version
check_version() {
    local image=$1
    local current_tag="${image##*:}"
    local image_without_tag="${image%%:*}"
    
    if [ "$current_tag" = "$image" ]; then
        current_tag="latest"
    fi
    
    echo -e "${CYAN}==================================${NC}"
    echo -e "${CYAN}Vérification de version${NC}"
    echo -e "${CYAN}==================================${NC}"
    echo -e "Image:       ${YELLOW}$image_without_tag${NC}"
    echo -e "Tag actuel:  ${YELLOW}$current_tag${NC}"
    echo ""
    
    # Vérifier si l'image existe localement
    if docker inspect "$image" >/dev/null 2>&1; then
        local created=$(docker inspect --format='{{.Created}}' "$image" | cut -d'.' -f1)
        local size=$(docker inspect --format='{{.Size}}' "$image" | awk '{printf "%.2f MB", $1/1024/1024}')
        echo -e "${GREEN}Image locale trouvée:${NC}"
        echo -e "  Créée: $created"
        echo -e "  Taille: $size"
        echo ""
    else
        echo -e "${YELLOW}Image non présente localement${NC}"
        echo ""
    fi
    
    # Récupérer les informations distantes
    echo -e "${BLUE}Informations distantes:${NC}"
    
    # Méthode 1: Vérifier le manifest (ignorer les erreurs d'accès)
    if docker manifest inspect "$image" >/dev/null 2>&1; then
        local remote_digest=$(docker manifest inspect "$image" 2>/dev/null | grep -m1 '"digest"' | awk '{print $2}' | tr -d ',"')
        echo -e "  Digest: ${remote_digest:0:20}..."
        
        # Extraire les métadonnées du manifest
        local config_digest=$(docker manifest inspect "$image" 2>/dev/null | jq -r '.config.digest' 2>/dev/null || echo "N/A")
        echo -e "  Config: ${config_digest:0:20}..."
    else
        echo -e "  ${YELLOW}⚠ Impossible d'accéder aux informations distantes (image locale uniquement ou accès refusé)${NC}"
    fi
    
    echo ""
    
    # Récupérer les tags disponibles
    echo -e "${BLUE}Tags récents disponibles:${NC}"
    get_available_tags "$image_without_tag"
    
    echo ""
    echo -e "${CYAN}==================================${NC}"
    echo -e "${GREEN}Commandes utiles:${NC}"
    echo ""
    echo -e "  ${BLUE}# Voir toutes les versions disponibles${NC}"
    echo -e "  docker search $image_without_tag --limit 25"
    echo ""
    echo -e "  ${BLUE}# Tirer une version spécifique${NC}"
    echo -e "  docker pull $image_without_tag:2024.12.0"
    echo ""
    echo -e "  ${BLUE}# Comparer avec latest${NC}"
    echo -e "  docker pull $image_without_tag:latest"
    echo -e "  docker images $image_without_tag"
    echo ""
    echo -e "  ${BLUE}# Voir l'historique d'une image${NC}"
    echo -e "  docker history $image"
    echo -e "${CYAN}==================================${NC}"
}

# Fonction spéciale pour Home Assistant
check_homeassistant() {
    local arch="${1:-aarch64}"
    local image="homeassistant/${arch}-homeassistant"
    
    echo -e "${CYAN}==================================${NC}"
    echo -e "${CYAN}Vérification Home Assistant${NC}"
    echo -e "${CYAN}==================================${NC}"
    echo ""
    
    echo -e "${BLUE}Récupération des dernières versions...${NC}"
    
    # API GitHub pour les releases Home Assistant
    local releases=$(curl -s "https://api.github.com/repos/home-assistant/core/releases?per_page=10")
    
    if echo "$releases" | jq -e '.[0]' >/dev/null 2>&1; then
        echo -e "${GREEN}Dernières versions publiées:${NC}"
        echo ""
        echo "$releases" | jq -r '.[] | "\(.tag_name)\t\(.published_at)\t\(.name)"' | while IFS=$'\t' read -r tag date name; do
            echo -e "  ${YELLOW}$tag${NC} - $name ($(date -d "$date" '+%Y-%m-%d' 2>/dev/null || echo "$date"))"
        done
        
        # Version la plus récente
        local latest_version=$(echo "$releases" | jq -r '.[0].tag_name')
        echo ""
        echo -e "${GREEN}Version stable la plus récente: ${YELLOW}$latest_version${NC}"
        
        # Vérifier la version locale si elle existe
        if docker images "${image}:latest" --format "{{.Tag}}" | grep -q "latest"; then
            echo ""
            echo -e "${BLUE}Image locale:${NC}"
            docker images "$image" --format "table {{.Tag}}\t{{.CreatedAt}}\t{{.Size}}"
        fi
        
        echo ""
        echo -e "${CYAN}Commandes de mise à jour:${NC}"
        echo -e "  ${BLUE}# Version latest${NC}"
        echo -e "  docker pull $image:latest"
        echo ""
        echo -e "  ${BLUE}# Version spécifique${NC}"
        echo -e "  docker pull $image:$latest_version"
        echo ""
        echo -e "  ${BLUE}# Versions béta/dev${NC}"
        echo -e "  docker pull $image:dev"
        echo -e "  docker pull $image:beta"
        
    else
        echo -e "${RED}Impossible de récupérer les versions depuis GitHub${NC}"
    fi
    
    echo ""
    echo -e "${CYAN}==================================${NC}"
}

# Parser les arguments
COMPOSE_FILE=""
CHECK_ALL=false
IMAGE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--file)
            COMPOSE_FILE="$2"
            shift 2
            ;;
        -a|--all)
            CHECK_ALL=true
            shift
            ;;
        -h|--help)
            show_help
            ;;
        -*)
            echo -e "${RED}Option inconnue: $1${NC}"
            show_help
            ;;
        *)
            IMAGE="$1"
            shift
            ;;
    esac
done

# Exécution principale
if [ "$CHECK_ALL" = true ] && [ -n "$COMPOSE_FILE" ]; then
    # Vérifier toutes les images du compose file
    if [ ! -f "$COMPOSE_FILE" ]; then
        echo -e "${RED}Erreur: Le fichier $COMPOSE_FILE n'existe pas!${NC}"
        exit 1
    fi
    
    IMAGES=$(grep -E "^\s*image:" "$COMPOSE_FILE" | grep -v "^\s*#" | awk '{print $2}' | sort -u)
    
    for img in $IMAGES; do
        check_version "$img"
        echo ""
    done
    
elif [ -n "$IMAGE" ]; then
    # Vérifier une image spécifique
    if [[ "$IMAGE" == *"homeassistant"* ]] || [ "$IMAGE" = "ha" ]; then
        check_homeassistant
    else
        check_version "$IMAGE"
    fi
else
    show_help
fi
