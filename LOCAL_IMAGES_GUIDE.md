# Guide des Images Docker Locales

## 🎯 Problème : "Error response from daemon: pull access denied"

Cette erreur survient quand Docker essaie d'accéder à une image qui :
- N'existe pas sur un registry public
- Est une image locale personnalisée (comme `batmon-ha`)
- Nécessite une authentification

## ✅ Solutions

### 1. Images locales (buildées localement)

Pour les images que vous construisez localement comme `batmon-ha`, les scripts **ignoreront automatiquement** l'erreur d'accès.

#### Exemple avec batmon-ha

```bash
# Builder l'image localement
./buildDocker.sh

# L'image sera disponible localement sous :
# - batmon-ha:latest
# - batmon-ha:1.82.7 (ou votre version)

# Les scripts de vérification afficheront :
# ⚠ Impossible d'accéder aux informations distantes (image locale uniquement ou accès refusé)
# ✓ Image locale disponible
```

### 2. Vérifier uniquement les images locales

```bash
# Lister toutes vos images locales
docker images

# Lister une image spécifique
docker images batmon-ha

# Voir les détails
docker inspect batmon-ha:latest
```

### 3. Exemples dans compose.yml

#### Image locale (buildée par vous)
```yaml
services:
  batmon:
    image: batmon-ha:latest  # Image locale
    # ou
    image: batmon-ha:1.82.7  # Version spécifique
```

#### Image publique
```yaml
services:
  batmon:
    image: ghcr.io/fl4p/batmon-ha:latest  # Image publique
```

## 🔧 Scripts mis à jour

Les scripts `checkVersions.sh` et `checkDockerUpdates.sh` ont été modifiés pour :

### ✅ Ignorer les erreurs d'accès

```bash
# Avant (erreur)
Error response from daemon: pull access denied for batmon-ha

# Maintenant (message informatif)
⚠ Impossible d'accéder aux informations distantes (accès refusé ou image locale uniquement)
✓ Image locale disponible
```

### ✅ Vérifier les images locales

Les scripts vérifient d'abord si l'image existe localement :

```bash
./checkVersions.sh batmon-ha
```

Sortie :
```
==================================
Vérification de version
==================================
Image:       batmon-ha
Tag actuel:  latest

Image locale trouvée:
  Créée: 2026-02-06T12:00:00
  Taille: 523.45 MB

Informations distantes:
  ⚠ Impossible d'accéder aux informations distantes (image locale uniquement ou accès refusé)
  
Commandes utiles:
  # Voir l'historique d'une image
  docker history batmon-ha
==================================
```

## 📋 Workflows recommandés

### Pour images locales (batmon-ha)

```bash
# 1. Builder localement
./buildDocker.sh

# 2. Vérifier l'image locale
docker images batmon-ha

# 3. Tester l'image
docker run --rm batmon-ha:latest --version

# 4. Utiliser dans compose.yml
services:
  batmon:
    image: batmon-ha:latest
```

### Pour images publiques (Home Assistant, etc.)

```bash
# 1. Vérifier les versions disponibles
./checkVersions.sh homeassistant/aarch64-homeassistant

# 2. Vérifier les mises à jour
./checkDockerUpdates.sh

# 3. Mettre à jour
docker pull homeassistant/aarch64-homeassistant:latest
```

## 🔍 Distinguer images locales vs publiques

### Images locales
- Nom court sans registry : `batmon-ha`, `myapp`, `custom-image`
- Buildées avec `docker build` ou `./buildDocker.sh`
- Ne peuvent pas être "pullées" depuis un registry

### Images publiques
- Nom avec registry : `ghcr.io/user/image`, `docker.io/library/nginx`
- Ou nom d'organisation : `homeassistant/...`, `nginx`, `mysql`
- Peuvent être téléchargées avec `docker pull`

## 🛠️ Commandes utiles

### Gérer les images locales

```bash
# Lister toutes les images locales
docker images

# Filtrer par nom
docker images batmon-ha

# Voir les détails
docker inspect batmon-ha:latest

# Voir l'historique (layers)
docker history batmon-ha:latest

# Voir la taille totale
docker images batmon-ha --format "{{.Repository}}:{{.Tag}} - {{.Size}}"

# Supprimer une image locale
docker rmi batmon-ha:old-version

# Nettoyer les images non utilisées
docker image prune
```

### Tagger une image locale

```bash
# Après un build
docker tag batmon-ha:latest batmon-ha:1.82.7

# Pour un registry privé
docker tag batmon-ha:latest myregistry.com/batmon-ha:latest
```

### Sauvegarder/Restaurer une image locale

```bash
# Sauvegarder dans un fichier
docker save batmon-ha:latest -o batmon-ha-latest.tar

# Compresser
docker save batmon-ha:latest | gzip > batmon-ha-latest.tar.gz

# Restaurer
docker load -i batmon-ha-latest.tar
# ou
gunzip -c batmon-ha-latest.tar.gz | docker load
```

## 📊 Exemple de compose.yml mixte

```yaml
version: '3.8'

services:
  # Image locale (buildée par vous)
  batmon:
    image: batmon-ha:latest
    container_name: batmon-ha
    restart: unless-stopped
    # ... config ...

  # Image publique
  homeassistant:
    image: homeassistant/aarch64-homeassistant:latest
    container_name: homeassistant
    restart: unless-stopped
    # ... config ...

  # Image publique (Docker Hub)
  mqtt:
    image: eclipse-mosquitto:latest
    container_name: mqtt
    restart: unless-stopped
    # ... config ...
```

## 🔄 Workflow de développement

### 1. Développement local

```bash
# Builder fréquemment pendant le développement
./buildDocker.sh

# Tester
docker compose up -d batmon
docker compose logs -f batmon

# Rebuilder après modifications
./buildDocker.sh
docker compose restart batmon
```

### 2. Versions et tags

```bash
# Version de développement
docker tag batmon-ha:latest batmon-ha:dev

# Version stable
echo "1.83.0" > version.txt
./buildDocker.sh
# Crée batmon-ha:1.83.0 et batmon-ha:latest
```

### 3. Partage (optionnel)

```bash
# Pousser vers un registry privé
docker tag batmon-ha:latest myregistry.com/batmon-ha:1.83.0
docker push myregistry.com/batmon-ha:1.83.0

# Ou sauvegarder pour partage fichier
docker save batmon-ha:latest | gzip > batmon-ha.tar.gz
```

## ⚠️ Points importants

1. **Les images locales ne peuvent pas être "pullées"** - elles doivent être buildées
2. **Les scripts ignorent maintenant les erreurs d'accès** - pas de panique !
3. **Utilisez des tags de version** pour un meilleur suivi
4. **Documentez vos images locales** dans votre README ou compose.yml
5. **Sauvegardez vos images importantes** avant nettoyage

## 💡 Conseils

- **Nommage** : Utilisez des noms explicites pour vos images locales
- **Versioning** : Toujours tagger avec une version en plus de `latest`
- **Documentation** : Commentez dans compose.yml quelles images sont locales
- **Cleanup** : Nettoyez régulièrement les anciennes versions
- **Registry** : Considérez un registry privé pour les images partagées en équipe

## 🔗 Ressources

- **Docker Build** : https://docs.docker.com/engine/reference/commandline/build/
- **Docker Images** : https://docs.docker.com/engine/reference/commandline/images/
- **Docker Save/Load** : https://docs.docker.com/engine/reference/commandline/save/
- **Private Registry** : https://docs.docker.com/registry/deploying/
