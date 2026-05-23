# 📱 MongoDB & FastAPI - Complete Guide

## 📚 Table des Matières
1. [MongoDB Setup](#mongodb-setup)
2. [Comment Obtenir MONGO_URI](#comment-obtenir-mongo_uri)
3. [Configuration du Projet](#configuration-du-projet)
4. [Démarrage de l'API](#démarrage-de-lapi)
5. [Tous les Endpoints](#tous-les-endpoints)
6. [Testing avec Postman](#testing-avec-postman)
7. [Testing avec curl](#testing-avec-curl)
8. [Troubleshooting](#troubleshooting)

---

## 🗄️ MongoDB Setup

### Option 1: MongoDB Atlas (Cloud - Recommandé)

#### Étape 1: Créer un compte
1. Aller sur **[mongodb.com/cloud/atlas](https://www.mongodb.com/cloud/atlas)**
2. Cliquer sur **"Sign Up"**
3. Remplir: email, mot de passe, etc.
4. Vérifier l'email et se connecter

#### Étape 2: Créer un Cluster Gratuit
1. Dans le dashboard → **"Create Deployment"**
2. Sélectionner **"Free"** (M0, gratuit)
3. Choisir un provider: AWS, Google Cloud, ou Azure
4. Choisir une région proche
5. Cliquer **"Create Deployment"** et attendre (~5-10 minutes)

#### Étape 3: Créer un utilisateur database
1. Menu à gauche → **"Database Access"**
2. Cliquer **"Add New Database User"**
3. Remplir:
   - **Username**: `admin` (ou votre choix)
   - **Password**: Mot de passe fort (garder précieusement!)
   - **Built-in Role**: Sélectionner **"Atlas Admin"**
4. Cliquer **"Add User"**

#### Étape 4: Autoriser les connexions
1. Menu à gauche → **"Network Access"**
2. Cliquer **"Add IP Address"**
3. Choisir **"Allow Access from Anywhere"** (ou votre IP)
4. Cliquer **"Confirm"**

---

## 🔐 Comment Obtenir MONGO_URI

### Depuis MongoDB Atlas

#### Méthode 1: Via la Dashboard

1. Aller à **"Clusters"** (menu à gauche)
2. Cliquer le bouton **"Connect"** sur votre cluster
3. Choisir **"Drivers"**
4. Sélectionner:
   - **Language**: Python
   - **Version**: 3.6 or later
5. **Copier la connection string:**

```
mongodb+srv://admin:PASSWORD@cluster0.xxxxx.mongodb.net/?appName=AppName
```

**Remplacer `PASSWORD` par votre mot de passe réel!**

#### Méthode 2: Exemple complet

Si vous avez:
- **Username**: `admin`
- **Password**: `MySecurePassword123`
- **Cluster URL**: `cluster0.abc123.mongodb.net`

Votre MONGO_URI sera:
```
mongodb+srv://admin:MySecurePassword123@cluster0.abc123.mongodb.net/?appName=AppName
```

### Pour MongoDB Local

Si MongoDB est installé localement:
```
mongodb://localhost:27017
```
.
---

## ⚙️ Configuration du Projet

### Étape 1: Créer le fichier `.env`

À la racine du projet, créer `.env`:

```powershell
# PowerShell
New-Item -Path ".env" -ItemType File
```

### Étape 2: Ajouter votre MONGO_URI

Éditer `.env` et ajouter:

```env
MONGO_URI=mongodb+srv://admin:YourPassword@cluster0.xxxxx.mongodb.net/?appName=AppName
```

### Étape 3: Vérifier la configuration

```powershell
# Activer le venv
& myvenv\Scripts\Activate.ps1

# Afficher le MONGO_URI
Get-Content .env

# Tester la connexion MongoDB
python -c "
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
uri = os.getenv('MONGO_URI')
client = MongoClient(uri)
client.admin.command('ping')
print('✓ MongoDB Connection OK!')
"
```

---

## ▶️ Démarrage de l'API

### Commande:

```powershell
# Activer le venv
& myvenv\Scripts\Activate.ps1

# Démarrer l'API
myvenv\Scripts\python.exe api/main.py
```

### Output attendu:

```
INFO:     Started server process [12345]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### ✅ L'API est prête quand tu vois: `Application startup complete`

### Accès:

- **API**: http://localhost:8000
- **Swagger UI (Docs)**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## 📡 Tous les Endpoints

### 1️⃣ HEALTH CHECK

#### `GET /`

**Description:** Vérifier le statut de l'API

**Request:**
```http
GET http://localhost:8000/
```

**Response:**
```json
{
  "message": "E-commerce Recommendation API",
  "status": "running",
  "version": "1.0.0"
}
```

---

#### `GET /health`

**Description:** Vérifier la connexion MongoDB

**Request:**
```http
GET http://localhost:8000/health
```

**Response (Succès):**
```json
{
  "status": "healthy",
  "mongodb": "connected"
}
```

**Response (Erreur):**
```json
{
  "status": "healthy",
  "mongodb": "error: [Connection refused]"
}
```

---

### 2️⃣ RECOMMENDATIONS

#### `GET /users/{user_id}/recommendations`

**Description:** Obtenir les recommandations pour UN utilisateur

**Parameters:**
- `user_id` (path, required): ID de l'utilisateur

**Request Examples:**
```http
GET http://localhost:8000/users/1/recommendations
GET http://localhost:8000/users/2/recommendations
GET http://localhost:8000/users/123/recommendations
```

**Response (Succès):**
```json
{
  "user_id": 1,
  "recommendations": [
    {
      "product_id": 101,
      "score": 4.82
    },
    {
      "product_id": 205,
      "score": 4.61
    },
    {
      "product_id": 312,
      "score": 4.45
    }
  ]
}
```

**Response (Erreur - Utilisateur non trouvé):**
```json
{
  "detail": "No recommendations found for user 999"
}
```

**Status Codes:**
- `200`: Succès
- `404`: Utilisateur non trouvé

---

#### `GET /recommendations`

**Description:** Obtenir TOUTES les recommandations (avec pagination)

**Query Parameters:**
- `limit` (optional, default: 100): Nombre max de résultats

**Request Examples:**
```http
GET http://localhost:8000/recommendations
GET http://localhost:8000/recommendations?limit=10
GET http://localhost:8000/recommendations?limit=50
GET http://localhost:8000/recommendations?limit=100
```

**Response:**
```json
[
  {
    "user_id": 1,
    "recommendations": [
      {"product_id": 101, "score": 4.82},
      {"product_id": 205, "score": 4.61}
    ]
  },
  {
    "user_id": 2,
    "recommendations": [
      {"product_id": 300, "score": 4.91},
      {"product_id": 450, "score": 4.55}
    ]
  },
  {
    "user_id": 3,
    "recommendations": [
      {"product_id": 205, "score": 4.73},
      {"product_id": 401, "score": 4.42}
    ]
  }
]
```

**Status Codes:**
- `200`: Succès

---

#### `GET /recommendations/stats`

**Description:** Obtenir les STATISTIQUES des recommandations

**Request:**
```http
GET http://localhost:8000/recommendations/stats
```

**Response:**
```json
{
  "total_users": 3,
  "avg_score_sample": 4.72,
  "mongodb_collection": "recommendations"
}
```

**Interprétation:**
- `total_users`: Nombre total d'utilisateurs avec recommandations
- `avg_score_sample`: Score moyen (sur un échantillon de 10)
- `mongodb_collection`: Nom de la collection MongoDB

---

## 📊 Résumé des Endpoints

| Endpoint | Method | Description | Parameters |
|----------|--------|-------------|------------|
| `/` | GET | Status de l'API | - |
| `/health` | GET | Check MongoDB | - |
| `/users/{user_id}/recommendations` | GET | Recommandations pour 1 user | `user_id` (path) |
| `/recommendations` | GET | Toutes les recommandations | `limit` (query) |
| `/recommendations/stats` | GET | Statistiques | - |

---

## 🧪 Testing avec Postman

### Import Postman Collection (Optional)

Vous pouvez importer le fichier `api/Postman_Collection.json` si disponible:
1. Ouvrir Postman
2. Cliquer **"Import"**
3. Sélectionner `api/Postman_Collection.json`

### Créer manuellement (Recommandé)

#### 1️⃣ Health Check

```
Name: Get API Status
Method: GET
URL: http://localhost:8000/
```

✅ Cliquer "Send" → Devrait retourner le statut de l'API

#### 2️⃣ Check MongoDB

```
Name: Check MongoDB Connection
Method: GET
URL: http://localhost:8000/health
```

✅ Cliquer "Send" → Devrait voir `"mongodb": "connected"`

#### 3️⃣ Get User 1 Recommendations

```
Name: Get User 1 Recommendations
Method: GET
URL: http://localhost:8000/users/1/recommendations
```

✅ Cliquer "Send" → Retourne recommandations pour user 1

#### 4️⃣ Get User 2 Recommendations

```
Name: Get User 2 Recommendations
Method: GET
URL: http://localhost:8000/users/2/recommendations
```

✅ Cliquer "Send" → Retourne recommandations pour user 2

#### 5️⃣ Get All Recommendations

```
Name: Get All Recommendations
Method: GET
URL: http://localhost:8000/recommendations
```

✅ Cliquer "Send" → Retourne toutes les recommandations

#### 6️⃣ Get Recommendations with Limit

```
Name: Get Recommendations (limit=10)
Method: GET
URL: http://localhost:8000/recommendations?limit=10
```

✅ Cliquer "Send" → Retourne max 10 résultats

#### 7️⃣ Get Statistics

```
Name: Get Statistics
Method: GET
URL: http://localhost:8000/recommendations/stats
```

✅ Cliquer "Send" → Retourne les stats

---

## 🔧 Testing avec curl (PowerShell)

### 1️⃣ Health Check

```powershell
# Check API Status
curl http://localhost:8000/

# Check MongoDB Connection
curl http://localhost:8000/health
```

### 2️⃣ Get Recommendations

```powershell
# User 1 recommendations
curl http://localhost:8000/users/1/recommendations

# User 2 recommendations
curl http://localhost:8000/users/2/recommendations

# All recommendations
curl http://localhost:8000/recommendations

# All recommendations (limit 10)
curl http://localhost:8000/recommendations?limit=10

# Statistics
curl http://localhost:8000/recommendations/stats
```

### 3️⃣ Pretty Print JSON (PowerShell)

```powershell
# Get et afficher joliment
$response = curl http://localhost:8000/recommendations/stats
$response | ConvertFrom-Json | ConvertTo-Json
```

### 4️⃣ Sauvegarde réponse dans un fichier

```powershell
# Sauvegarder dans un fichier
curl http://localhost:8000/recommendations > recommendations.json

# Afficher le fichier
Get-Content recommendations.json | ConvertFrom-Json | ConvertTo-Json
```

---

## 🆘 Troubleshooting

### ❌ Erreur: `"mongodb": "error"`

**Problème:** Connexion MongoDB échouée

**Solutions:**

```powershell
# 1. Vérifier le .env
Get-Content .env

# 2. Vérifier la syntaxe du MONGO_URI
# Devrait ressembler à:
# mongodb+srv://username:password@cluster.xxxxx.mongodb.net/?appName=AppName

# 3. Vérifier sur MongoDB Atlas
# - Database Access: Vérifier username/password
# - Network Access: "Allow Access from Anywhere"

# 4. Tester la connexion Python
python << 'EOF'
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
uri = os.getenv('MONGO_URI')
print(f"MONGO_URI: {uri}")

try:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("✓ MongoDB connection successful!")
except Exception as e:
    print(f"✗ Connection failed: {e}")
EOF
```

---

### ❌ Erreur: `404 - User not found`

**Problème:** L'utilisateur n'existe pas dans MongoDB

**Solutions:**

```powershell
# 1. Vérifier quels utilisateurs existent
curl http://localhost:8000/recommendations

# 2. Voir le nombre total d'utilisateurs
curl http://localhost:8000/recommendations/stats

# 3. Utiliser un user_id qui existe
curl http://localhost:8000/users/1/recommendations
```

---

### ❌ Erreur: `Connection refused`

**Problème:** L'API n'est pas lancée

**Solution:**

```powershell
# 1. Démarrer l'API
& myvenv\Scripts\Activate.ps1
myvenv\Scripts\python.exe api/main.py

# 2. Attendre le message: "Application startup complete"

# 3. Dans une autre fenêtre, tester
curl http://localhost:8000/
```

---

### ❌ Erreur: `Module not found`

**Problème:** Les dépendances ne sont pas installées

**Solution:**

```powershell
# Installer/réinstaller
& myvenv\Scripts\Activate.ps1
myvenv\Scripts\pip install -r requirements.txt
```

---

### ❌ Port 8000 déjà utilisé

**Problème:** Un autre processus utilise le port 8000

**Solution:**

```powershell
# Option 1: Tuer le processus qui utilise le port 8000
# Trouver le PID
netstat -ano | findstr :8000

# Tuer le processus (remplacer PID)
taskkill /PID 12345 /F

# Option 2: Utiliser un autre port
myvenv\Scripts\python.exe -m uvicorn api.main:app --port 8001
```

---

## 📊 Structure MongoDB

### Collection: `recommendations`

Chaque document dans MongoDB a cette structure:

```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "user_id": 1,
  "recommendations": [
    {
      "product_id": 101,
      "score": 4.82
    },
    {
      "product_id": 205,
      "score": 4.61
    }
  ]
}
```

### Database: `ecommerce_recommendation`

```
ecommerce_recommendation/
├── recommendations
│   ├── user_id: 1
│   ├── user_id: 2
│   └── user_id: 3
```

---

## 🎯 Quick Start Complet

```powershell
# 1. Activer le venv
& myvenv\Scripts\Activate.ps1

# 2. Vérifier .env existe et contient MONGO_URI
Get-Content .env

# 3. Tester connexion MongoDB
python -c "from pymongo import MongoClient; from dotenv import load_dotenv; import os; load_dotenv(); MongoClient(os.getenv('MONGO_URI')).admin.command('ping'); print('✓ OK')"

# 4. Démarrer l'API
myvenv\Scripts\python.exe api/main.py

# ========== Dans une autre fenêtre PowerShell ==========

# 5. Tester les endpoints
curl http://localhost:8000/health
curl http://localhost:8000/recommendations/stats
curl http://localhost:8000/users/1/recommendations

# 6. Ouvrir Swagger UI (optionnel)
# http://localhost:8000/docs
```

---

## 📚 Ressources Utiles

- **MongoDB Atlas**: https://www.mongodb.com/cloud/atlas
- **MongoDB Documentation**: https://docs.mongodb.com
- **PyMongo**: https://pymongo.readthedocs.io
- **FastAPI**: https://fastapi.tiangolo.com
- **Postman**: https://www.postman.com/downloads/

---

**✨ Vous êtes prêt à utiliser l'API!** 🚀
