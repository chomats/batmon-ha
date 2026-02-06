# Guide de Vérification des Versions Docker

## 🎯 Objectif

Ce guide explique comment vérifier si les images Docker (notamment Home Assistant) ont des mises à jour disponibles et comment identifier les changements de version.

## 🛠️ Outils disponibles

### 1. checkVersions.sh - Vérification détaillée des versions

Script intelligent pour vérifier les versions disponibles d'une image Docker.

#### Utilisation

```bash
# Vérifier Home Assistant (raccourci)
./checkVersions.sh ha

# Vérifier Home Assistant avec architecture spécifique
./checkVersions.sh homeassistant/aarch64-homeassistant

# Vérifier une autre image
./checkVersions.sh ghcr.io/fl4p/batmon-ha

# Vérifier toutes les images d'un fichier compose.yml
./checkVersions.sh -f doc/compose.yml -a
```

#### Fonctionnalités Home Assistant

Pour Home Assistant, le script :
- ✅ Récupère les 10 dernières versions depuis GitHub
- ✅ Affiche la version stable la plus récente
- ✅ Compare avec votre version locale
- ✅ Propose les commandes de mise à jour
- ✅ Montre les versions beta/dev disponibles

#### Exemple de sortie

```
==================================
Vérification Home Assistant
==================================

Récupération des dernières versions...

Dernières versions publiées:

  2024.12.5 - Home Assistant Core 2024.12.5 (2024-12-19)
  2024.12.4 - Home Assistant Core 2024.12.4 (2024-12-13)
  2024.12.3 - Home Assistant Core 2024.12.3 (2024-12-11)
  2024.12.2 - Home Assistant Core 2024.12.2 (2024-12-07)
  2024.12.1 - Home Assistant Core 2024.12.1 (2024-12-05)
  2024.12.0 - Home Assistant Core 2024.12.0 (2024-12-04)

Version stable la plus récente: 2024.12.5

Commandes de mise à jour:
  # Version latest
  docker pull homeassistant/aarch64-homeassistant:latest

  # Version spécifique
  docker pull homeassistant/aarch64-homeassistant:2024.12.5

  # Versions béta/dev
  docker pull homeassistant/aarch64-homeassistant:dev
  docker pull homeassistant/aarch64-homeassistant:beta
==================================
```

### 2. checkDockerUpdates.sh - Vérification des mises à jour

Script pour vérifier si vos images locales ont des mises à jour (compare les digests).

```bash
# Vérifier les images dans compose.yml
./checkDockerUpdates.sh

# Vérifier un autre fichier
./checkDockerUpdates.sh path/to/compose.yml
```

## 📋 Méthodes de vérification

### Méthode 1 : API GitHub (Home Assistant)

```bash
# Récupérer les dernières releases
curl -s "https://api.github.com/repos/home-assistant/core/releases?per_page=10" | jq -r '.[] | .tag_name'
```

### Méthode 2 : Docker Hub API

```bash
# Pour une image Docker Hub
curl -s "https://hub.docker.com/v2/repositories/homeassistant/aarch64-homeassistant/tags?page_size=25&ordering=last_updated" | jq -r '.results[] | "\(.name)\t\(.last_updated)"'
```

### Méthode 3 : Docker Manifest

```bash
# Comparer les digests
LOCAL=$(docker inspect --format='{{.RepoDigests}}' homeassistant/aarch64-homeassistant:latest)
REMOTE=$(docker manifest inspect homeassistant/aarch64-homeassistant:latest | grep -m1 digest)

echo "Local:  $LOCAL"
echo "Remote: $REMOTE"
```

### Méthode 4 : Docker Images

```bash
# Lister toutes les versions locales
docker images homeassistant/aarch64-homeassistant

# Voir les détails
docker inspect homeassistant/aarch64-homeassistant:latest | jq '.[0].Config.Labels'
```

## 🏠 Cas spécifique : Home Assistant

### Architectures disponibles

- `homeassistant/home-assistant` - Multi-architecture (amd64, arm64, armv7)
- `homeassistant/amd64-homeassistant` - Architecture x86_64
- `homeassistant/aarch64-homeassistant` - Architecture ARM 64 bits (Raspberry Pi 4+)
- `homeassistant/armv7-homeassistant` - Architecture ARM 32 bits v7 (Raspberry Pi 3)
- `homeassistant/armhf-homeassistant` - Architecture ARM 32 bits v6 (Raspberry Pi 1/2)

### Vérifier votre architecture

```bash
# Architecture du système
uname -m

# Architecture Docker
docker version --format '{{.Server.Arch}}'
```

Correspondances :
- `x86_64` → utilisez `amd64-homeassistant`
- `aarch64` → utilisez `aarch64-homeassistant`
- `armv7l` → utilisez `armv7-homeassistant`

### Versions disponibles

Home Assistant propose plusieurs canaux :

```bash
# Stable (recommandé)
docker pull homeassistant/aarch64-homeassistant:latest
docker pull homeassistant/aarch64-homeassistant:2024.12.5

# Beta (avant stable)
docker pull homeassistant/aarch64-homeassistant:beta

# Dev (développement, peut être instable)
docker pull homeassistant/aarch64-homeassistant:dev

# Version spécifique
docker pull homeassistant/aarch64-homeassistant:2024.12.5
```

### Calendrier de releases Home Assistant

- **Major release** : Premier mercredi du mois
- **Bug fix releases** : Selon les besoins (généralement 1-2 semaines après)
- **Beta** : ~1 semaine avant la major release

### Ressources officielles

- **Release notes** : https://www.home-assistant.io/blog/categories/release-notes/
- **GitHub releases** : https://github.com/home-assistant/core/releases
- **Docker Hub** : https://hub.docker.com/r/homeassistant/home-assistant
- **Version actuelle** : https://version.home-assistant.io/stable.json

## 🔄 Workflow de mise à jour

### Vérification hebdomadaire recommandée

```bash
# 1. Vérifier les nouvelles versions
./checkVersions.sh ha

# 2. Lire les release notes
# Visitez : https://www.home-assistant.io/blog/

# 3. Vérifier les breaking changes
# Important avant toute mise à jour majeure !

# 4. Faire un backup (si applicable)

# 5. Mettre à jour
docker pull homeassistant/aarch64-homeassistant:latest

# 6. Redémarrer le conteneur
docker compose restart homeassistant
# ou
docker restart homeassistant
```

### Mise à jour prudente (version spécifique)

```bash
# 1. Identifier la version cible
./checkVersions.sh ha

# 2. Tirer la version spécifique (ex: 2024.12.5)
docker pull homeassistant/aarch64-homeassistant:2024.12.5

# 3. Tester avec cette version
docker run -d --name ha-test homeassistant/aarch64-homeassistant:2024.12.5

# 4. Si OK, mettre à jour le compose.yml ou relancer
```

### Rollback en cas de problème

```bash
# 1. Lister les images disponibles localement
docker images homeassistant/aarch64-homeassistant

# 2. Identifier l'ancien digest/version
docker inspect homeassistant/aarch64-homeassistant:latest

# 3. Revenir à une version précédente
docker pull homeassistant/aarch64-homeassistant:2024.11.3
docker compose up -d  # ou docker restart
```

## 🤖 Automatisation

### Cron pour vérification quotidienne

```bash
# Éditer crontab
crontab -e

# Ajouter (vérification tous les jours à 8h)
0 8 * * * /path/to/checkVersions.sh ha > /tmp/ha-version-check.log 2>&1

# Avec notification email
0 8 * * * /path/to/checkVersions.sh ha | mail -s "Home Assistant Version Check" user@example.com
```

### Script de mise à jour automatique (à utiliser avec précaution)

```bash
#!/bin/bash
# auto-update-ha.sh

# Vérifier et mettre à jour Home Assistant
IMAGE="homeassistant/aarch64-homeassistant:latest"

# Récupérer les digests
LOCAL=$(docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null || echo "none")
docker pull "$IMAGE" > /dev/null 2>&1
REMOTE=$(docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE")

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "Mise à jour disponible, redémarrage..."
    docker compose restart homeassistant
    echo "Mise à jour effectuée le $(date)" >> /var/log/ha-updates.log
else
    echo "Déjà à jour"
fi
```

## 📊 Monitoring des versions

### Créer un dashboard de suivi

```bash
#!/bin/bash
# version-dashboard.sh

echo "=== Dashboard Versions Docker ==="
echo ""

# Home Assistant
echo "📱 Home Assistant:"
./checkVersions.sh ha | grep "Version stable"

# Batmon
echo ""
echo "🔋 Batmon:"
docker images ghcr.io/fl4p/batmon-ha --format "  Tag: {{.Tag}} | Créé: {{.CreatedAt}}"

# MQTT (si applicable)
echo ""
echo "📨 MQTT:"
docker images eclipse-mosquitto --format "  Tag: {{.Tag}} | Créé: {{.CreatedAt}}"

echo ""
echo "==================================="
```

## 🔗 Liens utiles

- **Home Assistant Release Calendar** : https://www.home-assistant.io/blog/
- **Breaking Changes** : https://www.home-assistant.io/blog/categories/breaking-changes/
- **Community Forum** : https://community.home-assistant.io/
- **GitHub Issues** : https://github.com/home-assistant/core/issues

## 💡 Conseils

1. **Toujours lire les release notes** avant une mise à jour majeure
2. **Faire des backups** réguliers de votre configuration
3. **Tester sur un environnement de test** si possible
4. **Ne pas utiliser `latest` en production** - préférez des versions spécifiques
5. **Attendre 2-3 jours** après une release majeure (pour les bug fixes)
6. **S'abonner aux notifications** GitHub pour être alerté des nouvelles versions
7. **Utiliser les beta versions** uniquement pour tester, pas en production
