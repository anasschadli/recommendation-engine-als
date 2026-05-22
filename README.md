# Moteur de recommandation e-commerce avec Spark MLlib ALS

Ce dossier contient la partie **Personne 2 - ML Engineer Spark ALS** du projet Big Data.

L'objectif est de prendre le fichier nettoye `ratings_clean.csv`, d'entrainer un modele de recommandation avec **PySpark MLlib ALS**, puis de produire le fichier principal `recommendations.json` pour l'equipe Backend.

## Structure

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
```

## Fichier d'entree attendu

Le script attend un CSV avec en-tete :

```csv
user_id,product_id,rating
1,101,5
1,203,4
2,101,3
2,305,5
```

Colonnes obligatoires :

- `user_id` : identifiant utilisateur, convertible en entier.
- `product_id` : identifiant produit, convertible en entier.
- `rating` : note ou score d'interaction, convertible en nombre.

Le fichier de test `data/ratings_clean.csv` contient 30 lignes pour lancer la partie ALS meme si le nettoyage Data n'est pas encore termine.

## Fichier de sortie produit

Le resultat principal est `outputs/recommendations.json` :

```json
[
  {
    "user_id": 1,
    "recommendations": [
      { "product_id": 101, "score": 4.82 },
      { "product_id": 205, "score": 4.61 }
    ]
  }
]
```

Ce fichier peut ensuite etre lu par le Backend pour insertion dans MongoDB Atlas, par exemple avec un document par utilisateur :

```json
{
  "user_id": 1,
  "recommendations": [
    { "product_id": 101, "score": 4.82 },
    { "product_id": 205, "score": 4.61 }
  ]
}
```

FastAPI pourra ensuite exposer une route du type `GET /recommendations/{user_id}`.

## Prerequis local

- Java installe et accessible depuis le terminal.
- Apache Spark installe, avec `spark-submit` disponible dans le `PATH`.
- PySpark disponible dans l'environnement utilise par Spark.

Pour un test local simple, on peut aussi installer PySpark dans un environnement Python :

```powershell
pip install pyspark
```

## Commandes d'execution locale

Depuis la racine du projet :

```powershell
spark-submit scripts/train_als.py
```

Avec parametres explicites :

```powershell
spark-submit scripts/train_als.py `
  --input data/ratings_clean.csv `
  --output outputs/recommendations.json `
  --model-output models/als_model `
  --top-n 5 `
  --rank 10 `
  --max-iter 10 `
  --reg-param 0.1
```

Pour generer un top 10 :

```powershell
spark-submit scripts/train_als.py --top-n 10
```

Pour ne pas sauvegarder le modele :

```powershell
spark-submit scripts/train_als.py --no-save-model
```

## Parametres ALS recommandes pour commencer

| Parametre | Valeur conseillee | Role |
|---|---:|---|
| `rank` | `10` | Nombre de facteurs latents utilisateur/produit. |
| `maxIter` | `10` | Nombre d'iterations d'entrainement. |
| `regParam` | `0.1` | Regularisation pour limiter le surapprentissage. |
| `userCol` | `user_id` | Colonne utilisateur. |
| `itemCol` | `product_id` | Colonne produit. |
| `ratingCol` | `rating` | Colonne note ou interaction. |
| `coldStartStrategy` | `drop` | Supprime les predictions impossibles sur nouveaux users/items pendant l'evaluation. |

Le script utilise aussi `nonnegative=True`, ce qui force des facteurs positifs et donne souvent des scores plus faciles a expliquer pour des notes e-commerce positives.

## Etapes du script

1. Lecture de `ratings_clean.csv` avec Spark.
2. Verification des colonnes obligatoires `user_id`, `product_id`, `rating`.
3. Suppression des lignes nulles ou non convertibles.
4. Conversion de `user_id` et `product_id` en entiers, et `rating` en nombre.
5. Aggregation des doublons utilisateur-produit avec la moyenne des notes.
6. Split du dataset en train/test.
7. Entrainement d'un modele ALS avec Spark MLlib sur le split train.
8. Evaluation du modele avec RMSE sur le split test.
9. Reentrainement du modele final sur toutes les interactions valides.
10. Generation du top N recommandations par utilisateur.
11. Conversion en JSON imbrique.
12. Sauvegarde de `outputs/recommendations.json`.
13. Sauvegarde optionnelle du modele dans `models/als_model`.

## Interpretation du RMSE

Le **RMSE** mesure l'ecart moyen entre les notes reelles du jeu de test et les notes predites par ALS.

- RMSE plus faible : predictions plus proches des notes reelles.
- RMSE plus eleve : predictions moins precises.

Pour un prototype academique, l'objectif n'est pas d'obtenir un score parfait, mais de montrer que le pipeline fonctionne et que le modele apprend des relations utilisateur-produit. Avec un mini dataset, le RMSE peut varier fortement car il y a peu d'interactions.

## Adaptation Dataproc / Cloud Storage

Le script peut lire une entree distante si le connecteur Hadoop approprie est disponible, par exemple sur Dataproc :

```bash
spark-submit scripts/train_als.py \
  --input gs://mon-bucket/data/ratings_clean.csv \
  --output gs://mon-bucket/outputs/recommendations.json \
  --model-output gs://mon-bucket/models/als_model \
  --top-n 10
```

Dans Airflow, cette commande peut etre appelee par un operateur Spark ou Dataproc. Le fichier `recommendations.json` devient ensuite l'artefact consomme par l'etape MongoDB Atlas / FastAPI.
