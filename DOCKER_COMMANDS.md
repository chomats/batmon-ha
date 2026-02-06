# Commandes Docker Utiles

## 🔍 Vérifier la plateforme Docker

### Plateforme par défaut du serveur Docker
```bash
docker version --format '{{.Server.Os}}/{{.Server.Arch}}'
```
Exemple de sortie : `linux/amd64`

### Plateforme du client Docker
```bash
docker version --format '{{.Client.Os}}/{{.Client.Arch}}'
```

### Informations complètes
```bash
docker version
```

### Architecture du système
```bash
uname -m
```
Correspondances :
- `x86_64` → `linux/amd64`
- `aarch64` → `linux/arm64`
- `armv7l` → `linux/arm/v7`
- `armhf` → `linux/arm/v6`

## 🐳 Plateformes supportées

### Vérifier les plateformes disponibles avec buildx
```bash
docker buildx ls
```

### Créer un builder multi-plateforme
```bash
docker buildx create --name multiplatform --use
docker buildx inspect --bootstrap
```

### Plateformes communes
- `linux/amd64` - Architecture x86 64 bits (Intel/AMD)
- `linux/arm64` - Architecture ARM 64 bits (Apple Silicon, Raspberry Pi 4+)
- `linux/arm/v7` - Architecture ARM 32 bits v7 (Raspberry Pi 2/3)
- `linux/arm/v6` - Architecture ARM 32 bits v6 (Raspberry Pi 1)
- `linux/386` - Architecture x86 32 bits
- `linux/s390x` - IBM Z Systems
- `linux/ppc64le` - PowerPC 64 bits Little Endian

## 📦 Utilisation de buildDocker.sh

### Build basique (utilise la plateforme détectée automatiquement)
```bash
./buildDocker.sh
```

### Build pour une plateforme spécifique
```bash
./buildDocker.sh -p linux/amd64
```

### Build multi-plateforme
```bash
./buildDocker.sh -p linux/amd64,linux/arm64 --push
```

### Build et push vers un registry
```bash
./buildDocker.sh -r ghcr.io/username -n batmon-ha --push
```

### Afficher l'aide
```bash
./buildDocker.sh -h
```

## 🏷️ Gestion des versions

### Voir la version actuelle
```bash
cat version.txt
```

### Mettre à jour la version
```bash
echo "1.82.7" > version.txt
```

## 📊 Inspection d'images

### Voir les plateformes d'une image
```bash
docker buildx imagetools inspect <image-name>
```

### Voir toutes les images locales
```bash
docker images
```

### Informations détaillées sur une image
```bash
docker inspect <image-name>
```

## 🧹 Nettoyage

### Supprimer les images non utilisées
```bash
docker image prune -a
```

### Supprimer le cache de build
```bash
docker buildx prune
```

### Nettoyer tout (attention !)
```bash
docker system prune -a --volumes
```

## 🚀 Exemples pratiques

### Build local pour test
```bash
./buildDocker.sh
docker run --rm batmon-ha:latest
```

### Build pour Raspberry Pi depuis un Mac/PC
```bash
./buildDocker.sh -p linux/arm64
```

### Build et déploiement complet
```bash
# 1. Mettre à jour la version
echo "1.83.0" > version.txt

# 2. Builder et pousser
./buildDocker.sh -r ghcr.io/username -n batmon-ha --push

# 3. Vérifier le déploiement
docker buildx imagetools inspect ghcr.io/username/batmon-ha:1.83.0
```

## ⚙️ Configuration buildx avancée

### Créer un builder avec driver docker-container
```bash
docker buildx create \
  --name mybuilder \
  --driver docker-container \
  --bootstrap \
  --use
```

### Lister les builders
```bash
docker buildx ls
```

### Supprimer un builder
```bash
docker buildx rm mybuilder
```

## 🔐 Authentification Registry

### GitHub Container Registry
```bash
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
```

### Docker Hub
```bash
docker login
```

### Registry privé
```bash
docker login registry.example.com
```

## 🔄 Vérifier les mises à jour d'images

### Utiliser le script checkDockerUpdates.sh
```bash
# Vérifier les images dans doc/compose.yml
./checkDockerUpdates.sh

# Vérifier un autre fichier compose
./checkDockerUpdates.sh path/to/compose.yml
```

### Méthode manuelle : Comparer les digests

#### Vérifier le digest local
```bash
docker inspect --format='{{.RepoDigests}}' <image-name>
```

#### Vérifier le digest distant
```bash
docker manifest inspect <image-name> | grep digest
```

#### Exemple complet
```bash
# Image locale
docker inspect --format='{{.RepoDigests}}' ghcr.io/fl4p/batmon-ha:latest

# Image distante
docker manifest inspect ghcr.io/fl4p/batmon-ha:latest | grep -m1 digest
```

### Docker Compose : Pull et mise à jour

#### Vérifier les mises à jour disponibles
```bash
cd doc
docker compose pull --quiet
docker compose ps
```

#### Mettre à jour tous les services
```bash
docker compose pull
docker compose up -d
```

#### Mettre à jour un service spécifique
```bash
docker compose pull batmon
docker compose up -d batmon
```

### Utiliser docker-compose avec --no-cache

#### Pull forcé (ignorer le cache)
```bash
docker compose pull --no-cache
```

### Watchtower : Mise à jour automatique

#### Installation
```bash
docker run -d \
  --name watchtower \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower \
  --interval 86400  # Vérifier toutes les 24h
```

#### Avec notification
```bash
docker run -d \
  --name watchtower \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e WATCHTOWER_NOTIFICATIONS=email \
  -e WATCHTOWER_NOTIFICATION_EMAIL_FROM=from@example.com \
  -e WATCHTOWER_NOTIFICATION_EMAIL_TO=to@example.com \
  containrrr/watchtower
```

### Diun : Alternative à Watchtower (notification uniquement)

```bash
docker run -d \
  --name diun \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(pwd)/diun.yml:/diun.yml:ro \
  crazymax/diun:latest
```

### Vérifier l'âge d'une image

```bash
# Date de création de l'image locale
docker inspect --format='{{.Created}}' <image-name>

# Voir les informations complètes
docker image inspect <image-name>
```

### Exemples pratiques

#### Script de vérification quotidienne (cron)
```bash
# Ajouter à crontab -e
0 2 * * * /path/to/checkDockerUpdates.sh && echo "Vérification terminée" | mail -s "Docker Updates" user@example.com
```

#### Mettre à jour et redémarrer
```bash
# Un seul service
docker compose pull batmon && docker compose up -d batmon

# Tous les services
docker compose pull && docker compose up -d
```

#### Vérifier sans mettre à jour
```bash
# Liste les images qui peuvent être mises à jour
docker images --format "table {{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedSince}}"
```
