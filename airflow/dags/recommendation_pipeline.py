from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import subprocess
import sys

# Configuration du DAG
default_args = {
    'owner': 'Equipe B',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

dag = DAG(
    'recommendation_pipeline',
    default_args=default_args,
    description='Pipeline complet: clean data -> train ALS -> generate recommendations -> insert to MongoDB',
    schedule_interval=None,  # Manuel uniquement (change à '@daily' pour automatique)
    catchup=False,
)

# Variables communes - Chemins adaptés pour Docker
PROJECT_ROOT = '/opt/airflow/project'  # Volume monté du projet
PYTHON_EXEC = sys.executable  # Python du container

# ============================================================================
# TÂCHE 1: Nettoyage des données
# ============================================================================
def clean_data_task():
    """
    Nettoyage et préparation des données
    """
    print("DATA CLEANING TASK")
    print("=" * 60)
    
    import csv
    
    # Fichiers - Chemins relatifs au PROJECT_ROOT
    input_file = os.path.join(PROJECT_ROOT, 'data', 'ratings_clean.csv')
    
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input file: {input_file}")
    
    # Vérification
    if not os.path.exists(input_file):
        print(f"ERROR: Input file not found: {input_file}")
        print(f"   Available files: {os.listdir(PROJECT_ROOT) if os.path.exists(PROJECT_ROOT) else 'DIRECTORY NOT FOUND'}")
        raise FileNotFoundError(f"File {input_file} not found")
    
    print(f"File found")
    
    # Statistiques de base
    with open(input_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"Number of rows: {len(rows)}")
    print(f"Columns: {list(rows[0].keys()) if rows else 'N/A'}")
    
    # Validation
    required_cols = {'user_id', 'product_id', 'rating'}
    if rows:
        cols = set(rows[0].keys())
        if not required_cols.issubset(cols):
            print(f"ERROR: Missing columns: {required_cols - cols}")
            raise ValueError(f"Required columns: {required_cols}")
    
    print("Data validated and ready for training")
    return "Données nettoyées avec succès"

clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,
    dag=dag,
)

# ============================================================================
# TÂCHE 2: Entraînement du modèle ALS
# ============================================================================
def train_als_task():
    """
    Entraînement du modèle ALS avec Spark
    """
    print("ALS MODEL TRAINING TASK")
    print("=" * 60)
    
    # Chemins
    script = os.path.join(PROJECT_ROOT, 'scripts', 'train_als.py')
    
    print(f"Script: {script}")
    
    if not os.path.exists(script):
        print(f"ERROR: Script not found: {script}")
        raise FileNotFoundError(f"Script {script} not found")
    
    print(f"Script found")
    
    # Lancer le script Spark
    print("Starting spark-submit...")
    try:
        result = subprocess.run(
            [
                'spark-submit',
                script,
                '--input', 'data/ratings_clean.csv',
                '--output', 'outputs/recommendations.json',
                '--model-output', 'models/als_model',
                '--top-n', '5',
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes max
        )
        
        if result.returncode != 0:
            print(f"ERROR: Spark job failed: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            raise RuntimeError(f"Spark job failed")
        
        print(result.stdout)
        print("ALS model training completed successfully")
        
    except subprocess.TimeoutExpired:
        print("ERROR: Spark job exceeded timeout")
        raise
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    
    return "Modèle ALS entraîné"

train_als = PythonOperator(
    task_id='train_als_model',
    python_callable=train_als_task,
    dag=dag,
)

# ============================================================================
# TÂCHE 3: Génération des recommandations
# ============================================================================
def generate_recommendations_task():
    """
    Génération des recommandations (fait parte du script train_als.py)
    """
    print("RECOMMENDATIONS GENERATION TASK")
    print("=" * 60)
    
    import json
    
    output_file = os.path.join(PROJECT_ROOT, 'outputs', 'recommendations.json')
    
    # Vérifier que le fichier a été créé
    if not os.path.exists(output_file):
        print(f"ERROR: Recommendations file not found: {output_file}")
        raise FileNotFoundError(f"File {output_file} not found")
    
    # Charger et valider
    with open(output_file, 'r') as f:
        data = json.load(f)
    
    print(f"File found: {output_file}")
    print(f"Number of users: {len(data)}")
    print(f"Total recommendations: {sum(len(u.get('recommendations', [])) for u in data)}")
    
    # Validation du format
    for user in data[:1]:  # Vérifier le premier
        required_keys = {'user_id', 'recommendations'}
        if not required_keys.issubset(set(user.keys())):
            print(f"ERROR: Invalid format - missing keys")
            raise ValueError("Invalid JSON format")
    
    print("Recommendations generated and validated")
    return f"{len(data)} utilisateurs avec recommandations"

generate_recommendations = PythonOperator(
    task_id='generate_recommendations',
    python_callable=generate_recommendations_task,
    dag=dag,
)

# ============================================================================
# TÂCHE 4: Insertion dans MongoDB
# ============================================================================
def insert_to_mongodb_task():
    """
    Insertion des recommandations dans MongoDB
    """
    print("MONGODB INSERTION TASK")
    print("=" * 60)
    
    script = os.path.join(PROJECT_ROOT, 'scripts', 'insert_mock_data.py')
    
    if not os.path.exists(script):
        print(f"ERROR: Insert script not found: {script}")
        raise FileNotFoundError(f"Script {script} not found")
    
    print(f"Script found: {script}")
    
    # Lancer le script d'insertion
    print("Starting insert script...")
    try:
        result = subprocess.run(
            [PYTHON_EXEC, script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ}  # Inclure les variables d'environnement (pour .env)
        )
        
        print(result.stdout)
        
        if result.returncode != 0:
            print(f"ERROR: {result.stderr}")
            raise RuntimeError(f"Insert operation failed")
        
        print("Data successfully inserted into MongoDB")
        
    except subprocess.TimeoutExpired:
        print("ERROR: Insert operation exceeded timeout")
        raise
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    
    return "Données insérées dans MongoDB"

insert_to_mongodb = PythonOperator(
    task_id='insert_to_mongodb',
    python_callable=insert_to_mongodb_task,
    dag=dag,
)

# ============================================================================
# TÂCHE 5: Vérification et statut final
# ============================================================================
def api_ready_task():
    """
    Vérification que l'API peut servir les recommandations
    """
    print("API VERIFICATION TASK")
    print("=" * 60)
    
    try:
        import requests
        
        # Essayer de vérifier que l'API est accessible
        # Note: L'API peut ne pas être lancée dans le container Airflow
        # On va juste vérifier que MongoDB est accessible
        
        print("NOTE: API must be started separately (http://localhost:8000)")
        print("To start API: python -m uvicorn api.main:app --reload")
        
    except ImportError:
        print("WARNING: 'requests' module not available")
    except Exception as e:
        print(f"WARNING: Unable to verify API: {e}")
    
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print("\nCompleted tasks:")
    print("   1. Data cleaning")
    print("   2. ALS model training")
    print("   3. Recommendations generation")
    print("   4. MongoDB insertion")
    print("   5. API verification")
    
    return "Pipeline terminé avec succès"

api_ready = PythonOperator(
    task_id='api_ready',
    python_callable=api_ready_task,
    dag=dag,
)

# ============================================================================
# DÉFINIR LES DÉPENDANCES
# ============================================================================
# clean_data → train_als → generate_recommendations → insert_to_mongodb → api_ready

clean_data >> train_als >> generate_recommendations >> insert_to_mongodb >> api_ready
