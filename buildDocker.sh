#!/bin/bash

# Script pour builder l'image Docker avec version depuis version.txt
# Usage: ./buildDocker.sh [options]
# Options:
#   -p, --platform    Plateforme cible (défaut: linux/arm64)
#   -r, --registry    Registry Docker (défaut: local)
#   -n, --name        Nom de l'image (défaut: batmon-ha)
#   --push           Pousser l'image vers le registry
#   -h, --help       Afficher l'aide

set -e

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Détecter la plateforme par défaut du système
DEFAULT_PLATFORM=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null || echo "linux/amd64")

# Valeurs par défaut
PLATFORM="${DEFAULT_PLATFORM}"
REGISTRY=""
IMAGE_NAME="batmon-ha"
PUSH=false
VERSION_FILE="version.txt"

# Fonction d'affichage de l'aide
show_help() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -p, --platform PLATFORM    Plateforme cible (défaut: linux/arm64)"
    echo "                             Exemples: linux/amd64, linux/arm64, linux/arm/v7"
    echo "  -r, --registry REGISTRY    Registry Docker (ex: ghcr.io/username)"
    echo "  -n, --name NAME            Nom de l'image (défaut: batmon-ha)"
    echo "  --push                     Pousser l'image vers le registry"
    echo "  -h, --help                 Afficher cette aide"
    echo ""
    echo "Exemples:"
    echo "  $0                                          # Build local pour arm64"
    echo "  $0 -p linux/amd64                          # Build pour amd64"
    echo "  $0 -r ghcr.io/user -n batmon --push        # Build et push vers registry"
    exit 0
}

# Parse les arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--platform)
            PLATFORM="$2"
            shift 2
            ;;
        -r|--registry)
            REGISTRY="$2"
            shift 2
            ;;
        -n|--name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        --push)
            PUSH=true
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo -e "${RED}Option inconnue: $1${NC}"
            show_help
            ;;
    esac
done

# Vérifier que le fichier version.txt existe
if [ ! -f "$VERSION_FILE" ]; then
    echo -e "${RED}Erreur: Le fichier $VERSION_FILE n'existe pas!${NC}"
    exit 1
fi

# Lire la version depuis le fichier
VERSION=$(cat "$VERSION_FILE" | tr -d '[:space:]')

if [ -z "$VERSION" ]; then
    echo -e "${RED}Erreur: La version est vide dans $VERSION_FILE${NC}"
    exit 1
fi

# Construire le nom complet de l'image
if [ -n "$REGISTRY" ]; then
    FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}"
else
    FULL_IMAGE_NAME="${IMAGE_NAME}"
fi

# Afficher les informations de build
echo -e "${GREEN}==================================${NC}"
echo -e "${GREEN}Build de l'image Docker${NC}"
echo -e "${GREEN}==================================${NC}"
echo -e "Image:      ${YELLOW}${FULL_IMAGE_NAME}${NC}"
echo -e "Version:    ${YELLOW}${VERSION}${NC}"
echo -e "Plateforme: ${YELLOW}${PLATFORM}${NC}"
echo -e "Push:       ${YELLOW}${PUSH}${NC}"
echo -e "${GREEN}==================================${NC}"
echo ""

# Builder l'image
echo -e "${GREEN}Building Docker image...${NC}"
docker buildx build \
    --platform "${PLATFORM}" \
    --tag "${FULL_IMAGE_NAME}:${VERSION}" \
    --tag "${FULL_IMAGE_NAME}:latest" \
    $([ "$PUSH" = true ] && echo "--push" || echo "--load") \
    .

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}==================================${NC}"
    echo -e "${GREEN}✓ Build réussi!${NC}"
    echo -e "${GREEN}==================================${NC}"
    echo -e "Image: ${YELLOW}${FULL_IMAGE_NAME}:${VERSION}${NC}"
    echo -e "Image: ${YELLOW}${FULL_IMAGE_NAME}:latest${NC}"
    
    if [ "$PUSH" = false ]; then
        echo ""
        echo -e "${YELLOW}Pour pousser l'image vers le registry:${NC}"
        echo -e "  docker push ${FULL_IMAGE_NAME}:${VERSION}"
        echo -e "  docker push ${FULL_IMAGE_NAME}:latest"
    fi
else
    echo -e "${RED}Erreur lors du build!${NC}"
    exit 1
fi
