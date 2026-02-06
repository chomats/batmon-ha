# Guide des Volumes Docker

## 🎯 Objectif

Savoir si un volume Docker est utilisé et identifier ses points de montage.

## 📋 Commandes essentielles

### 1. Lister tous les volumes

```bash
# Liste simple
docker volume ls

# Liste détaillée avec taille
docker system df -v

# Avec filtres
docker volume ls --filter "dangling=false"  # Volumes utilisés
docker volume ls --filter "dangling=true"   # Volumes non utilisés
```

### 2. Inspecter un volume spécifique

```bash
# Détails complets d'un volume
docker volume inspect <volume-name>

# Exemple
docker volume inspect grafana-storage

# Format JSON lisible
docker volume inspect grafana-storage | jq
```

**Sortie exemple :**
```json
[
  {
    "CreatedAt": "2026-02-06T12:00:00Z",
    "Driver": "local",
    "Labels": {
      "com.docker.compose.project": "doc",
      "com.docker.compose.version": "2.24.5",
      "com.docker.compose.volume": "grafana-storage"
    },
    "Mountpoint": "/var/lib/docker/volumes/grafana-storage/_data",
    "Name": "grafana-storage",
    "Options": null,
    "Scope": "local"
  }
]
```

### 3. Trouver quel conteneur utilise un volume

```bash
# Méthode 1 : Via docker ps
docker ps -a --filter volume=<volume-name>

# Méthode 2 : Inspecter tous les conteneurs
docker ps -a --format '{{.Names}}' | xargs -I {} sh -c 'docker inspect {} | grep -q "grafana-storage" && echo {}'

# Méthode 3 : Avec jq (plus propre)
docker ps -a --format '{{.Names}}' | while read container; do
  docker inspect $container | jq -r --arg vol "grafana-storage" '.[] | select(.Mounts[].Name == $vol) | .Name'
done
```

### 4. Voir tous les points de montage d'un conteneur

```bash
# Méthode simple
docker inspect <container-name> | grep -A 20 "Mounts"

# Avec jq (formaté)
docker inspect <container-name> | jq '.[].Mounts'

# Exemple
docker inspect grafana | jq '.[].Mounts'
```

**Sortie exemple :**
```json
[
  {
    "Type": "volume",
    "Name": "grafana-storage",
    "Source": "/var/lib/docker/volumes/grafana-storage/_data",
    "Destination": "/var/lib/grafana",
    "Driver": "local",
    "Mode": "rw",
    "RW": true,
    "Propagation": ""
  }
]
```

### 5. Lister tous les montages (volumes + bind mounts)

```bash
# Pour un conteneur spécifique
docker inspect <container-name> --format='{{range .Mounts}}{{.Type}}: {{.Source}} -> {{.Destination}} ({{.Mode}}){{println}}{{end}}'

# Exemple avec grafana
docker inspect grafana --format='{{range .Mounts}}{{.Type}}: {{.Source}} -> {{.Destination}} ({{.Mode}}){{println}}{{end}}'
```

**Sortie exemple :**
```
volume: /var/lib/docker/volumes/grafana-storage/_data -> /var/lib/grafana (rw)
```

## 🔍 Vérification détaillée

### Script complet pour analyser un volume

```bash
#!/bin/bash
VOLUME_NAME="$1"

if [ -z "$VOLUME_NAME" ]; then
    echo "Usage: $0 <volume-name>"
    exit 1
fi

echo "==================================="
echo "Analyse du volume: $VOLUME_NAME"
echo "==================================="
echo ""

# 1. Vérifier si le volume existe
if ! docker volume inspect "$VOLUME_NAME" >/dev/null 2>&1; then
    echo "❌ Le volume '$VOLUME_NAME' n'existe pas"
    exit 1
fi

echo "✅ Volume trouvé"
echo ""

# 2. Point de montage sur l'hôte
MOUNTPOINT=$(docker volume inspect "$VOLUME_NAME" --format '{{.Mountpoint}}')
echo "📁 Point de montage hôte:"
echo "   $MOUNTPOINT"
echo ""

# 3. Taille du volume
if [ -d "$MOUNTPOINT" ]; then
    SIZE=$(du -sh "$MOUNTPOINT" 2>/dev/null | cut -f1)
    echo "💾 Taille: $SIZE"
else
    echo "⚠️  Impossible d'accéder au point de montage (droits root requis)"
fi
echo ""

# 4. Conteneurs utilisant ce volume
echo "🐳 Conteneurs utilisant ce volume:"
CONTAINERS=$(docker ps -a --format '{{.Names}}' | while read container; do
    docker inspect "$container" 2>/dev/null | jq -r --arg vol "$VOLUME_NAME" '.[] | select(.Mounts[]?.Name == $vol) | .Name' 2>/dev/null
done)

if [ -z "$CONTAINERS" ]; then
    echo "   ⚠️  Aucun conteneur n'utilise ce volume (volume orphelin)"
else
    echo "$CONTAINERS" | while read container; do
        STATUS=$(docker inspect "$container" --format '{{.State.Status}}')
        MOUNT_PATH=$(docker inspect "$container" | jq -r --arg vol "$VOLUME_NAME" '.[].Mounts[] | select(.Name == $vol) | .Destination')
        echo "   ✓ $container ($STATUS) → $MOUNT_PATH"
    done
fi
echo ""

# 5. Labels
echo "🏷️  Labels:"
docker volume inspect "$VOLUME_NAME" --format '{{json .Labels}}' | jq -r 'to_entries[] | "   \(.key) = \(.value)"' 2>/dev/null || echo "   Aucun label"
echo ""

echo "==================================="
```

Sauvegardez ce script comme `checkVolume.sh` et utilisez :
```bash
chmod +x checkVolume.sh
./checkVolume.sh grafana-storage
```

## 📊 Commandes Docker Compose

### Lister les volumes d'un projet Compose

```bash
# Dans le dossier du compose.yml
cd doc

# Lister les volumes du projet
docker compose config --volumes

# Voir la configuration complète
docker compose config | grep -A 5 "volumes:"
```

### Inspecter les volumes d'un service

```bash
# Voir les montages d'un service
docker compose ps grafana --format json | jq '.[].Mounts'

# Ou avec inspect
docker inspect doc-grafana-1 | jq '.[].Mounts'
```

## 🔧 Exemples pratiques

### Exemple 1 : Vérifier grafana-storage

```bash
# Est-ce que le volume existe ?
docker volume inspect grafana-storage

# Quel conteneur l'utilise ?
docker ps -a --filter volume=grafana-storage

# Où est-il monté dans le conteneur ?
docker inspect grafana | jq '.[].Mounts[] | select(.Name=="grafana-storage") | {Destination, Source, Mode}'
```

**Résultat attendu :**
```json
{
  "Destination": "/var/lib/grafana",
  "Source": "/var/lib/docker/volumes/grafana-storage/_data",
  "Mode": "rw"
}
```

### Exemple 2 : Vérifier prometheus_data

```bash
# Inspection complète
docker volume inspect prometheus_data

# Conteneur utilisant ce volume
docker ps -a | grep prometheus

# Point de montage
docker inspect prometheus | jq '.[].Mounts'
```

### Exemple 3 : Bind mount vs Volume

```bash
# Pour batmon_ha (bind mount)
docker inspect batmon_ha | jq '.[].Mounts'
```

**Sortie bind mount :**
```json
[
  {
    "Type": "bind",
    "Source": "/home/user/doc/options.json",
    "Destination": "/options.json",
    "Mode": "",
    "RW": true,
    "Propagation": "rprivate"
  }
]
```

**Sortie volume nommé :**
```json
[
  {
    "Type": "volume",
    "Name": "grafana-storage",
    "Source": "/var/lib/docker/volumes/grafana-storage/_data",
    "Destination": "/var/lib/grafana",
    "Driver": "local",
    "Mode": "rw",
    "RW": true,
    "Propagation": ""
  }
]
```

## 🗑️ Gestion des volumes

### Identifier les volumes non utilisés

```bash
# Lister les volumes orphelins (dangling)
docker volume ls --filter "dangling=true"

# Voir l'espace utilisé
docker system df -v | grep -A 100 "Local Volumes"
```

### Nettoyer les volumes non utilisés

```bash
# Supprimer tous les volumes orphelins
docker volume prune

# Avec confirmation
docker volume prune -f

# Supprimer un volume spécifique
docker volume rm <volume-name>
```

⚠️ **Attention :** Les données seront PERDUES définitivement !

### Sauvegarder un volume

```bash
# Créer une archive du contenu d'un volume
docker run --rm -v grafana-storage:/source -v $(pwd):/backup alpine tar czf /backup/grafana-storage-backup.tar.gz -C /source .

# Ou avec sudo pour accès direct
sudo tar czf grafana-storage-backup.tar.gz -C /var/lib/docker/volumes/grafana-storage/_data .
```

### Restaurer un volume

```bash
# Créer le volume
docker volume create grafana-storage

# Restaurer depuis l'archive
docker run --rm -v grafana-storage:/target -v $(pwd):/backup alpine tar xzf /backup/grafana-storage-backup.tar.gz -C /target

# Ou avec sudo
sudo tar xzf grafana-storage-backup.tar.gz -C /var/lib/docker/volumes/grafana-storage/_data
```

## 📝 Tableau récapitulatif

| Type | Déclaration dans compose.yml | Point de montage hôte | Persistance |
|------|----------------------------|----------------------|-------------|
| **Volume nommé** | `volumes: grafana-storage:/var/lib/grafana` | `/var/lib/docker/volumes/grafana-storage/_data` | ✅ Oui |
| **Volume anonyme** | `volumes: /var/lib/grafana` | `/var/lib/docker/volumes/<hash>/_data` | ⚠️ Orphelin si supprimé |
| **Bind mount** | `volumes: ./options.json:/options.json` | Chemin exact spécifié | ✅ Fichier/dossier hôte |

## 🎯 Cas d'usage de votre compose.yml

### Volumes nommés définis

```yaml
volumes:
  grafana-storage: {}
  prometheus_data: {}
```

### Vérification

```bash
# 1. Lister
docker volume ls | grep -E "grafana-storage|prometheus_data"

# 2. Vérifier qui utilise grafana-storage
docker ps -a | grep grafana
docker inspect grafana | jq '.[].Mounts'

# 3. Vérifier qui utilise prometheus_data
docker ps -a | grep prometheus
docker inspect prometheus | jq '.[].Mounts'

# 4. Taille des volumes
docker system df -v | grep -E "grafana-storage|prometheus_data"
```

### Bind mounts dans votre config

```yaml
batmon_ha:
  volumes:
    - ./options.json:/options.json  # Bind mount

homeassistant:
  volumes:
    - /var/lib/homeassistant/homeassistant:/config  # Bind mount
```

Pour vérifier :
```bash
docker inspect batmon_ha | jq '.[].Mounts[] | {Type, Source, Destination}'
docker inspect homeassistant | jq '.[].Mounts[] | {Type, Source, Destination}'
```

## 🔗 Commandes rapides

```bash
# Voir TOUS les volumes
docker volume ls

# Voir TOUS les montages d'un conteneur
docker inspect <container> | jq '.[].Mounts'

# Trouver les volumes orphelins
docker volume ls -qf dangling=true

# Espace disque utilisé par les volumes
docker system df -v

# Point de montage d'un volume
docker volume inspect <volume> --format '{{.Mountpoint}}'

# Conteneurs utilisant un volume
docker ps -a --filter volume=<volume-name>
```

## 💡 Conseils

1. **Utilisez des volumes nommés** pour les données importantes
2. **Documentez vos bind mounts** dans des commentaires
3. **Sauvegardez régulièrement** les volumes critiques
4. **Nettoyez les volumes orphelins** périodiquement
5. **Vérifiez les permissions** si vous accédez directement aux points de montage

## 🔗 Ressources

- **Docker Volumes** : https://docs.docker.com/storage/volumes/
- **Bind Mounts** : https://docs.docker.com/storage/bind-mounts/
- **Docker Compose Volumes** : https://docs.docker.com/compose/compose-file/07-volumes/
