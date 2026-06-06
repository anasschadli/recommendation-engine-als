from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json
from datetime import datetime

# Charger les variables d'environnement
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    print("❌ Erreur : MONGO_URI non définie dans .env")
    exit(1)

print(f"📡 Connexion à MongoDB...")

try:
    client = MongoClient(MONGO_URI)
    # Vérifier la connexion
    client.admin.command('ping')
    print("✅ Connexion MongoDB réussie")
except Exception as e:
    print(f"❌ Erreur de connexion MongoDB : {e}")
    exit(1)

db = client["ecommerce_recommendation"]
collection = db["recommendations"]

# Chercher le fichier JSON
json_file = "outputs/recommendations.json"
if not os.path.exists(json_file):
    # Essayer depuis le dossier scripts
    json_file = os.path.join("..", json_file)

if not os.path.exists(json_file):
    print(f"❌ Erreur : Fichier JSON introuvable ({json_file})")
    exit(1)

print(f"📂 Chargement depuis : {json_file}")

try:
    with open(json_file, "r") as file:
        data = json.load(file)
    
    # Ajouter timestamp si absent
    for doc in data:
        if "generated_at" not in doc:
            doc["generated_at"] = datetime.now().isoformat()
    
    # Insérer les données
    collection.delete_many({})
    result = collection.insert_many(data)
    
    print(f"✅ {len(result.inserted_ids)} documents insérés avec succès !")
    print(f"📊 Collection '{collection.name}' contient maintenant {collection.count_documents({})} documents")
    
except Exception as e:
    print(f"❌ Erreur lors de l'insertion : {e}")