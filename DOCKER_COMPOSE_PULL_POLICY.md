# Docker Compose : Politique de Pull (pull_policy)

## 🎯 Objectif

Contrôler quand Docker Compose télécharge (pull) les images pour éviter les erreurs "pull access denied" avec les images locales.

## 📋 Options disponibles

### `pull_policy: never`
**Ne jamais faire de pull - utilise uniquement l'image locale**

```yaml
services:
  batmon:
    image: batmon-ha:latest
    pull_policy: never  # Utilise UNIQUEMENT l'image locale
```

✅ **Utilisation :**
- Images buildées localement
- Images importées manuellement
- Développement local

❌ **Erreur si :** L'image n'existe pas localement

---

### `pull_policy: if_not_present` (défaut)
**Pull seulement si l'image n'existe pas localement**

```yaml
services:
  homeassistant:
    image: homeassistant/aarch64-homeassistant:latest
    pull_policy: if_not_present  # Pull si absent, sinon utilise le local
```

✅ **Utilisation :**
- Comportement par défaut
- Images publiques
- Développement et production

---

### `pull_policy: always`
**Toujours pull avant de démarrer**

```yaml
services:
  homeassistant:
    image: homeassistant/aarch64-homeassistant:latest
    pull_policy: always  # Vérifie toujours les mises à jour
```

✅ **Utilisation :**
- Production avec tag `:latest`
- Besoin de la version la plus récente
- CI/CD

⚠️ **Attention :** Ralentit le démarrage

---

### `pull_policy: missing` 
**Alias pour `if_not_present`**

```yaml
services:
  app:
    image: myapp:latest
    pull_policy: missing  # Identique à if_not_present
```

---

### `pull_policy: build`
**Builder au lieu de pull**

```yaml
services:
  app:
    build: .
    pull_policy: build  # Force le build
```

✅ **Utilisation :**
- Avec l'option `build:`
- Développement actif

---

## 🎨 Exemples pratiques

### Exemple 1 : Image locale uniquement (batmon-ha)

```yaml
services:
  batmon_ha:
    image: batmon-ha:latest
    pull_policy: never  # ← Empêche docker pull
    container_name: batmon_ha
    restart: unless-stopped
    devices:
      - /dev/ttyUSB0
```

**Avantages :**
- ✅ Pas d'erreur "pull access denied"
- ✅ Démarrage rapide
- ✅ Contrôle total de la version

---

### Exemple 2 : Mix images locales et publiques

```yaml
services:
  # Image locale
  batmon_ha:
    image: batmon-ha:latest
    pull_policy: never
    # ... config ...

  # Image publique
  homeassistant:
    image: homeassistant/aarch64-homeassistant:2026.1.2
    pull_policy: if_not_present  # ou omis (défaut)
    # ... config ...

  # Image publique avec mise à jour automatique
  mqtt:
    image: eclipse-mosquitto:latest
    pull_policy: always  # Vérifie les mises à jour
    # ... config ...
```

---

### Exemple 3 : Build automatique

```yaml
services:
  batmon_ha:
    build:
      context: .
      dockerfile: Dockerfile
    image: batmon-ha:latest
    pull_policy: build
    # ... config ...
```

---

## 🔧 Commandes Docker Compose

### Avec pull_policy

```bash
# Démarrer sans pull (respecte pull_policy)
docker compose up -d

# Forcer le pull malgré pull_policy
docker compose pull
docker compose up -d

# Forcer le pull pour un service spécifique
docker compose pull batmon_ha
docker compose up -d batmon_ha
```

### Sans pull_policy (comportement par défaut)

```bash
# Pull uniquement si image absente
docker compose up -d

# Forcer le pull
docker compose up -d --pull always

# Ne jamais pull
docker compose up -d --pull never

# Pull seulement si absent
docker compose up -d --pull missing
```

---

## 📊 Tableau récapitulatif

| pull_policy | Pull si absent | Pull si présent | Use case |
|-------------|---------------|-----------------|----------|
| `never` | ❌ Erreur | ❌ Non | Image locale uniquement |
| `if_not_present` (défaut) | ✅ Oui | ❌ Non | Usage général |
| `always` | ✅ Oui | ✅ Oui | Production `:latest` |
| `missing` | ✅ Oui | ❌ Non | Alias de `if_not_present` |
| `build` | N/A | N/A | Avec option `build:` |

---

## 💡 Recommandations

### Pour images locales (batmon-ha)

```yaml
services:
  batmon_ha:
    image: batmon-ha:latest
    pull_policy: never  # ← RECOMMANDÉ
```

**Pourquoi ?**
- Évite l'erreur "pull access denied"
- Plus rapide au démarrage
- Pas de dépendance réseau

---

### Pour images publiques avec version spécifique

```yaml
services:
  homeassistant:
    image: homeassistant/aarch64-homeassistant:2026.1.2
    pull_policy: if_not_present  # ← RECOMMANDÉ (ou omis)
```

**Pourquoi ?**
- Pas besoin de pull si déjà présent
- Version fixe = reproductible

---

### Pour images publiques avec tag :latest

```yaml
services:
  mqtt:
    image: eclipse-mosquitto:latest
    pull_policy: always  # ← Si vous voulez toujours la dernière version
```

**Pourquoi ?**
- Obtenir les dernières mises à jour
- ⚠️ Peut ralentir le démarrage

---

## 🚫 Résoudre "pull access denied"

### Problème

```bash
docker compose up -d
# Error: pull access denied for batmon-ha
```

### Solution 1 : Ajouter pull_policy: never

```yaml
services:
  batmon_ha:
    image: batmon-ha:latest
    pull_policy: never  # ← Ajouter cette ligne
```

### Solution 2 : Utiliser --pull never

```bash
docker compose up -d --pull never
```

### Solution 3 : Builder l'image d'abord

```bash
# Builder l'image locale
./buildDocker.sh

# Puis démarrer
docker compose up -d
```

---

## 🔄 Migration vers pull_policy

### Avant (sans pull_policy)

```yaml
services:
  batmon_ha:
    image: batmon-ha:latest
    # Comportement par défaut : if_not_present
```

### Après (avec pull_policy explicite)

```yaml
services:
  batmon_ha:
    image: batmon-ha:latest
    pull_policy: never  # Image locale, ne jamais pull
```

---

## 📝 Exemple complet pour doc/compose.yml

```yaml
services:
  # Image locale buildée par ./buildDocker.sh
  batmon_ha:
    image: batmon-ha:latest
    pull_policy: never  # Ne jamais pull (image locale)
    container_name: batmon_ha
    restart: unless-stopped
    volumes:
      - ./options.json:/options.json
    devices:
      - /dev/ttyUSB0
    networks:
      - ha_networks

  # Image publique avec version fixe
  homeassistant:
    image: homeassistant/aarch64-homeassistant:2026.1.2
    pull_policy: if_not_present  # Pull si absent (défaut)
    container_name: homeassistant
    restart: unless-stopped
    # ... config ...

  # Image publique toujours à jour
  mqtt5:
    image: eclipse-mosquitto:latest
    pull_policy: always  # Toujours vérifier les mises à jour
    container_name: mqtt5
    restart: unless-stopped
    # ... config ...
```

---

## 🔗 Références

- **Docker Compose Spec** : https://docs.docker.com/compose/compose-file/05-services/#pull_policy
- **Docker Compose CLI** : https://docs.docker.com/compose/reference/

---

## ⚡ Commandes rapides

```bash
# Ajouter pull_policy: never à un service
sed -i '/image: batmon-ha/a\    pull_policy: never' compose.yml

# Démarrer sans pull
docker compose up -d --pull never

# Démarrer avec pull forcé
docker compose up -d --pull always

# Voir la config finale
docker compose config
```
