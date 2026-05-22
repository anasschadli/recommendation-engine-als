# Moteur de Recommandation E-commerce a Grande Echelle

Projet academique Cloud / Big Data dont l'objectif est de construire un prototype complet de moteur de recommandation e-commerce.

Le systeme prend des interactions utilisateur-produit, entraine un modele de filtrage collaboratif avec Apache Spark MLlib ALS, genere des recommandations personnalisees, les stocke dans MongoDB Atlas, puis les expose avec une API REST FastAPI. Le pipeline complet est prevu pour etre automatise avec Apache Airflow et executable localement ou sur GCP Dataproc.

## Methode de travail retenue

Nous n'avons pas retenu la premiere methode avec 5 personnes separees en chaine, car elle cree trop d'attente entre les etapes :

```text
Data -> ML -> Cloud -> MongoDB -> API -> Airflow
```

La methode retenue est :

```text
Deux equipes independantes + contrat commun des le jour 1
```

Cette organisation permet aux deux equipes de travailler en parallele presque tout le temps.

```text
Equipe A : Data + Spark ALS + Cloud
Objectif : produire outputs/recommendations.json

Equipe B : MongoDB + FastAPI + Airflow + Rapport
Objectif : consommer recommendations.json et exposer les recommandations via API
```

La seule integration obligatoire arrive a la fin : l'equipe B remplace son fichier mock par le vrai fichier `recommendations.json` produit par l'equipe A.

## Architecture globale

```text
Dataset e-commerce brut
        |
        v
Nettoyage des donnees
        |
        v
data/ratings_clean.csv
        |
        v
Spark MLlib ALS
        |
        v
outputs/recommendations.json
        |
        v
MongoDB Atlas
        |
        v
FastAPI
        |
        v
Client / Swagger UI
```

Airflow orchestre le pipeline :

```text
clean_data
    |
    v
train_als_model
    |
    v
generate_recommendations
    |
    v
insert_to_mongodb
    |
    v
api_ready
```

## Technologies

| Partie | Technologie | Role |
|---|---|---|
| Big Data / ML | Apache Spark MLlib ALS | Entrainement du modele de recommandation |
| Cloud | GCP Cloud Storage | Stockage des donnees, scripts, modeles et sorties |
| Execution Spark | GCP Dataproc ou local | Execution distribuee du job PySpark |
| Base de donnees | MongoDB Atlas | Stockage des recommandations finales |
| API | FastAPI | Exposition des recommandations via REST |
| Orchestration | Apache Airflow | Automatisation du pipeline |
| Bonus / perspective | Vertex AI | Extension possible pour gestion ML cloud |

## Contrats de donnees

Les deux equipes doivent respecter les contrats suivants.

### Entree du modele ALS

Fichier attendu :

```text
data/ratings_clean.csv
```

Structure obligatoire :

```csv
user_id,product_id,rating
1,101,5
1,203,4
2,101,3
2,305,5
```

Colonnes :

- `user_id` : identifiant utilisateur, convertible en entier.
- `product_id` : identifiant produit, convertible en entier.
- `rating` : note ou score d'interaction, convertible en nombre.

Le fichier peut etre petit pour les tests ou beaucoup plus grand pour la version finale. Tant que l'en-tete reste exactement `user_id,product_id,rating`, le script peut le lire.

### Sortie du modele ALS

Fichier produit :

```text
outputs/recommendations.json
```

Structure obligatoire :

```json
[
  {
    "user_id": 1,
    "recommendations": [
      {
        "product_id": 205,
        "score": 4.82
      },
      {
        "product_id": 306,
        "score": 4.61
      }
    ]
  }
]
```

Regles importantes :

- Un document JSON par utilisateur.
- Chaque utilisateur contient une liste `recommendations`.
- Chaque recommandation contient `product_id` et `score`.
- Les produits deja notes/interagis par l'utilisateur sont filtres avant l'export.
- Ce fichier est le contrat principal entre l'equipe A et l'equipe B.

### Document MongoDB

L'equipe B peut inserer chaque entree du JSON dans une collection MongoDB Atlas `recommendations`.

Exemple de document :

```json
{
  "user_id": 1,
  "recommendations": [
    {
      "product_id": 205,
      "score": 4.82
    },
    {
      "product_id": 306,
      "score": 4.61
    }
  ],
  "generated_at": "2026-05-22"
}
```

## Repartition des equipes

### Equipe A - Data / Spark ALS / Cloud

Membres : 3 personnes.

Objectif : produire les recommandations finales.

#### Personne 1 - Data Engineer

| Priorite | Taches |
|---|---|
| Haute | Trouver ou preparer un dataset e-commerce |
| Haute | Nettoyer les donnees |
| Haute | Produire les colonnes `user_id`, `product_id`, `rating` |
| Haute | Generer `data/ratings_clean.csv` |
| Moyenne | Calculer des statistiques : utilisateurs, produits, interactions |
| Faible | Ajouter des graphiques pour le rapport |

Livrables :

```text
data/ratings_clean.csv
statistiques du dataset
description du nettoyage
```

#### Personne 2 - ML Engineer Spark ALS

| Priorite | Taches |
|---|---|
| Haute | Developper `scripts/train_als.py` |
| Haute | Entrainer le modele ALS |
| Haute | Generer les recommandations top N par utilisateur |
| Haute | Exporter `outputs/recommendations.json` |
| Haute | Filtrer les produits deja vus par chaque utilisateur |
| Moyenne | Evaluer le modele avec RMSE |
| Faible | Tester plusieurs parametres ALS |

Livrables :

```text
scripts/train_als.py
outputs/recommendations.json
models/als_model/
RMSE et explication du modele
```

#### Personne 3 - Cloud / Dataproc

| Priorite | Taches |
|---|---|
| Haute | Preparer l'organisation locale ou cloud |
| Haute | Creer les dossiers `raw/`, `processed/`, `outputs/`, `scripts/`, `models/` |
| Haute | Tester l'execution Spark |
| Moyenne | Lancer le job sur Dataproc si possible |
| Moyenne | Documenter les commandes cloud |
| Faible | Presenter Vertex AI comme extension possible |

Livrables :

```text
structure Cloud Storage
commandes Dataproc
captures d'execution
documentation cloud
```

### Equipe B - MongoDB / FastAPI / Airflow / Rapport

Membres : 2 personnes.

Objectif : consommer `recommendations.json`, stocker les recommandations et les exposer via API.

#### Personne 4 - Backend MongoDB + FastAPI

| Priorite | Taches |
|---|---|
| Haute | Creer MongoDB Atlas |
| Haute | Creer la collection `recommendations` |
| Haute | Inserer `recommendations_mock.json` pendant le developpement |
| Haute | Remplacer le mock par `outputs/recommendations.json` a l'integration |
| Haute | Developper l'API FastAPI |
| Haute | Creer `GET /recommendations/{user_id}` |
| Moyenne | Ajouter `GET /health` |
| Moyenne | Tester l'API avec Swagger |
| Faible | Ajouter `GET /users` ou `GET /products/{product_id}` |

Livrables :

```text
API FastAPI
connexion MongoDB Atlas
script d'import JSON vers MongoDB
documentation Swagger
```

#### Personne 5 - Airflow / Integration / Rapport

| Priorite | Taches |
|---|---|
| Haute | Creer un DAG Airflow avec des taches fictives au debut |
| Haute | Definir le pipeline `clean -> train -> generate -> insert` |
| Haute | Preparer la structure du rapport |
| Haute | Preparer les slides |
| Moyenne | Remplacer les taches fictives par les vrais scripts |
| Moyenne | Tester le pipeline complet |
| Faible | Ajouter logs, captures ou monitoring simple |

Livrables :

```text
DAG Airflow
pipeline automatise
rapport technique
slides de presentation
captures de demonstration
```

## Structure de ce depot

Ce depot contient actuellement la partie Spark ALS de l'equipe A et le contrat de sortie consomme par l'equipe B.

```text
recommendation-engine-als/
+-- data/
|   +-- ratings_clean.csv
+-- models/
|   +-- als_model/
+-- outputs/
|   +-- recommendations.json
+-- scripts/
|   +-- train_als.py
+-- README.md
+-- pyproject.toml
+-- uv.lock
+-- note.txt
```

## Fonctionnement du script ALS

Le script principal est :

```text
scripts/train_als.py
```

Etapes :

1. Lire `ratings_clean.csv`.
2. Verifier les colonnes obligatoires `user_id`, `product_id`, `rating`.
3. Supprimer les lignes invalides ou non convertibles.
4. Convertir `user_id` et `product_id` en entiers, et `rating` en float.
5. Agreger les doublons utilisateur-produit avec la moyenne des notes.
6. Diviser les donnees en train/test.
7. Entrainer un modele ALS avec Spark MLlib.
8. Evaluer le modele avec RMSE si le jeu de test le permet.
9. Reentrainer le modele final sur toutes les interactions valides.
10. Generer des candidats de recommandation par utilisateur.
11. Supprimer les produits deja notes avec un `left_anti` join Spark.
12. Garder le top N final.
13. Exporter `outputs/recommendations.json`.
14. Sauvegarder le modele dans `models/als_model/` si l'option n'est pas desactivee.

Parametres ALS utilises par defaut :

| Parametre | Valeur | Role |
|---|---:|---|
| `rank` | `10` | Nombre de facteurs latents |
| `maxIter` | `10` | Nombre d'iterations |
| `regParam` | `0.1` | Regularisation |
| `coldStartStrategy` | `drop` | Ignore les predictions impossibles pendant l'evaluation |
| `nonnegative` | `True` | Force des facteurs positifs |

## Execution locale

Prerequis :

- Python 3.10 ou plus.
- Java 17.
- PySpark.

Le projet utilise actuellement Python `3.11` et PySpark via l'environnement local.

Installation des dependances avec `uv` :

```bash
uv sync
```

Execution simple :

```bash
spark-submit scripts/train_als.py
```

Execution avec parametres :

```bash
spark-submit scripts/train_als.py \
  --input data/ratings_clean.csv \
  --output outputs/recommendations.json \
  --model-output models/als_model \
  --top-n 5 \
  --rank 10 \
  --max-iter 10 \
  --reg-param 0.1
```

Execution sans sauvegarder le modele :

```bash
spark-submit scripts/train_als.py --no-save-model
```

Sur macOS, si Java 25 est actif, Spark peut echouer avec `getSubject is not supported`. Utiliser Java 17 :

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home \
PATH=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin:$PATH \
.venv/bin/python scripts/train_als.py
```

## Execution GCP / Dataproc

Organisation recommandee dans Cloud Storage :

```text
gs://bucket-recommendation/
+-- raw/
+-- processed/
|   +-- ratings_clean.csv
+-- scripts/
|   +-- train_als.py
+-- models/
|   +-- als_model/
+-- outputs/
    +-- recommendations.json
```

Commande type :

```bash
spark-submit scripts/train_als.py \
  --input gs://bucket-recommendation/processed/ratings_clean.csv \
  --output gs://bucket-recommendation/outputs/recommendations.json \
  --model-output gs://bucket-recommendation/models/als_model \
  --top-n 10
```

Sur Dataproc, le meme script peut etre lance comme job PySpark, a condition que le cluster ait acces au bucket et aux dependances necessaires.

## API attendue cote equipe B

Routes minimales :

```text
GET /health
GET /recommendations/{user_id}
```

Exemple :

```text
GET /recommendations/1
```

Reponse attendue :

```json
{
  "user_id": 1,
  "recommendations": [
    {
      "product_id": 205,
      "score": 4.82
    },
    {
      "product_id": 306,
      "score": 4.61
    }
  ]
}
```

Pendant le developpement, l'equipe B peut travailler avec un fichier :

```text
recommendations_mock.json
```

qui respecte exactement le meme format que `outputs/recommendations.json`.

## DAG Airflow attendu

Le DAG peut commencer avec des taches fictives, puis etre connecte aux vrais scripts.

Pipeline cible :

```text
start
  |
  v
clean_data
  |
  v
train_als_model
  |
  v
generate_recommendations
  |
  v
insert_recommendations_to_mongodb
  |
  v
end
```

Objectif Airflow :

- automatiser le nettoyage ;
- lancer le job Spark ;
- verifier la presence de `recommendations.json` ;
- importer les recommandations dans MongoDB ;
- permettre une demonstration claire du pipeline complet.

## Planning recommande sur 2 semaines

### Semaine 1

| Jour | Equipe A | Equipe B |
|---|---|---|
| J1 | Choix dataset + contrat final | Creation mock JSON + structure API |
| J2 | Nettoyage dataset | MongoDB Atlas + insertion mock |
| J3 | Script Spark ALS simple | Route `GET /recommendations/{user_id}` |
| J4 | Generation recommandations test | Swagger + tests API |
| J5 | Amelioration modele | DAG Airflow fictif |
| J6-J7 | Tests Spark / cloud | Rapport + captures API/MongoDB |

### Semaine 2

| Jour | Equipe A | Equipe B |
|---|---|---|
| J8 | Generation recommandations finales | Preparation integration |
| J9 | Export final `recommendations.json` | Import du vrai fichier dans MongoDB |
| J10 | Tests modele | Test API avec vraies donnees |
| J11 | Captures Spark/Dataproc | DAG Airflow avec vrais scripts |
| J12 | Correction bugs | Correction bugs |
| J13 | Aide rapport | Slides + demonstration |
| J14 | Repetition soutenance | Repetition soutenance |

## Demonstration finale

Pendant la soutenance, l'ordre de demonstration conseille est :

1. Montrer le dataset nettoye `ratings_clean.csv`.
2. Lancer ou presenter le job Spark ALS.
3. Montrer le RMSE et les logs principaux.
4. Montrer `outputs/recommendations.json`.
5. Montrer que les produits deja notes sont filtres.
6. Importer ou montrer les donnees dans MongoDB Atlas.
7. Tester `GET /recommendations/{user_id}` dans Swagger.
8. Montrer le DAG Airflow et l'ordre des taches.
9. Conclure avec les limites et perspectives.

## Limites du prototype

- Le dataset de test local est petit ; le RMSE peut varier fortement.
- Les recommandations sont batch, pas temps reel.
- ALS recommande seulement parmi les produits presents dans les donnees d'entrainement.
- Les nouveaux utilisateurs ou nouveaux produits necessitent une strategie cold start.
- Vertex AI est garde comme perspective, pas comme exigence principale du MVP.

## Conclusion

Le projet suit une architecture Big Data complete mais realiste pour 2 semaines :

```text
ratings_clean.csv
    -> Spark MLlib ALS
    -> recommendations.json
    -> MongoDB Atlas
    -> FastAPI
    -> Airflow
```

La cle du projet est le contrat commun `outputs/recommendations.json`. Grace a ce contrat, l'equipe A et l'equipe B peuvent avancer en parallele, puis integrer le systeme final sans bloquer le developpement.
