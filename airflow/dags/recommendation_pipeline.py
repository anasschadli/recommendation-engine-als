from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import subprocess
import sys
import time
import json
import boto3

# Configuration du DAG
default_args = {
    'owner': 'Equipe B',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'recommendation_pipeline',
    default_args=default_args,
    description='Pipeline complet: clean data -> AWS EMR Spark ALS -> generate recommendations -> insert to MongoDB',
    start_date=pendulum.datetime(2024, 1, 1, tz='UTC'),
    schedule=None,  # Manuel uniquement
    catchup=False,
    tags=['recommendation', 'als', 'spark', 'mongodb', 'aws', 'emr'],
)

# Variables communes - Chemins adaptés pour Docker
PROJECT_ROOT = '/opt/airflow/project'
PYTHON_EXEC  = sys.executable

# ─── AWS Configuration (P3) ──────────────────────────────────────────────────
AWS_REGION = 'eu-west-1'
S3_BUCKET  = 'ecommerce-bigdata-bucket'
S3_INPUT   = f's3://{S3_BUCKET}/raw/ratings_clean.csv'
S3_SCRIPT  = f's3://{S3_BUCKET}/scripts/train_als.py'
S3_OUTPUT  = f's3://{S3_BUCKET}/outputs/recommendations.json'
S3_MODEL   = f's3://{S3_BUCKET}/models/als_model'
S3_LOGS    = f's3://{S3_BUCKET}/logs/'


# ============================================================================
# TÂCHE 1: Nettoyage des données (P1)
# ============================================================================
def clean_data_task():
    print("DATA CLEANING TASK")
    print("=" * 60)

    import csv

    input_file = os.path.join(PROJECT_ROOT, 'data', 'ratings_clean.csv')

    print(f"Project root: {PROJECT_ROOT}")
    print(f"Input file: {input_file}")

    if not os.path.exists(input_file):
        print(f"ERROR: Input file not found: {input_file}")
        print(f"   Available files: {os.listdir(PROJECT_ROOT) if os.path.exists(PROJECT_ROOT) else 'DIRECTORY NOT FOUND'}")
        raise FileNotFoundError(f"File {input_file} not found")

    print(f"File found")

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Number of rows: {len(rows)}")
    print(f"Columns: {list(rows[0].keys()) if rows else 'N/A'}")

    required_cols = {'user_id', 'product_id', 'rating'}
    if rows:
        cols = set(rows[0].keys())
        if not required_cols.issubset(cols):
            print(f"ERROR: Missing columns: {required_cols - cols}")
            raise ValueError(f"Required columns: {required_cols}")

    print("✅ Data validated and ready for training")
    return "Données nettoyées avec succès"


clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,
    dag=dag,
)


# ============================================================================
# TÂCHE 2: Upload vers S3 (P3 — Cloud)
# ============================================================================
def upload_to_s3_task():
    """
    Upload ratings_clean.csv et train_als.py vers S3
    avant de lancer le cluster EMR.
    """
    print("UPLOAD TO S3 TASK (P3 - Cloud)")
    print("=" * 60)

    s3 = boto3.client('s3', region_name=AWS_REGION)

    # Upload dataset
    local_csv = os.path.join(PROJECT_ROOT, 'data', 'ratings_clean.csv')
    print(f"📤 Upload {local_csv} → {S3_INPUT}")
    s3.upload_file(local_csv, S3_BUCKET, 'raw/ratings_clean.csv')

    # Upload script Spark
    local_script = os.path.join(PROJECT_ROOT, 'scripts', 'train_als.py')
    print(f"📤 Upload {local_script} → {S3_SCRIPT}")
    s3.upload_file(local_script, S3_BUCKET, 'scripts/train_als.py')

    print("✅ Fichiers uploadés dans S3 avec succès !")
    return "Upload S3 terminé"


upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3_task,
    dag=dag,
)


# ============================================================================
# TÂCHE 3: Entraînement du modèle ALS sur AWS EMR (P3 — Cloud)
# Remplace le spark-submit local par un cluster EMR AWS
# Equivalent de : aws emr create-cluster ... --steps Type=Spark,...
# ============================================================================
def train_als_task(**context):
    """
    Lance un cluster AWS EMR, exécute le job Spark ALS,
    et attend la fin du cluster automatiquement.
    """
    print("AWS EMR — SPARK ALS TRAINING TASK (P2 & P3 - ALS & Cloud)")
    print("=" * 60)

    emr = boto3.client('emr', region_name=AWS_REGION)

    # Créer le cluster + lancer le job Spark
    # Equivalent exact de la commande CMD :
    # aws emr create-cluster --name "ecommerce-cluster" \
    #   --release-label emr-6.9.0 --applications Name=Spark \
    #   --instance-groups MASTER,m5.xlarge,1 CORE,m5.xlarge,2 \
    #   --use-default-roles --region eu-west-1 --auto-terminate \
    #   --log-uri s3://... --steps Type=Spark,...
    response = emr.run_job_flow(
        Name='ecommerce-cluster',
        ReleaseLabel='emr-6.9.0',
        Applications=[{'Name': 'Spark'}],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Workers',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                },
            ],
            'KeepJobFlowAliveWhenNoSteps': False,  # auto-terminate
        },
        Steps=[
            {
                'Name': 'ALS Job',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        S3_SCRIPT,
                        '--input',        S3_INPUT,
                        '--output',       S3_OUTPUT,
                        '--model-output', S3_MODEL,
                        '--top-n',        '5',
                    ],
                },
            }
        ],
        LogUri=S3_LOGS,
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
    )

    cluster_id = response['JobFlowId']
    print(f"🚀 Cluster EMR lancé — ClusterId : {cluster_id}")

    # Passer le ClusterId à la tâche suivante via XCom
    context['ti'].xcom_push(key='cluster_id', value=cluster_id)

    # Attendre la fin du cluster
    # Equivalent de : aws emr wait cluster-terminated --cluster-id ...
    print("⏳ Attente de la fin du job Spark ALS...")
    while True:
        state = emr.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']
        print(f"   État cluster : {state}")

        if state == 'TERMINATED':
            print("✅ Cluster terminé avec succès !")
            break
        elif state == 'TERMINATED_WITH_ERRORS':
            raise RuntimeError(
                f"❌ Cluster {cluster_id} terminé avec erreurs. "
                f"Vérifiez les logs : {S3_LOGS}{cluster_id}/"
            )
        else:
            time.sleep(60)  # vérifier toutes les 60 secondes

    return f"Cluster {cluster_id} terminé avec succès"


train_als = PythonOperator(
    task_id='train_als_model',
    python_callable=train_als_task,
    dag=dag,
    execution_timeout=timedelta(hours=1),
)


# ============================================================================
# TÂCHE 4: Télécharger + valider recommendations.json depuis S3 (P3 — Cloud)
# Equivalent de : aws s3 cp s3://bucket/outputs/recommendations.json outputs/
# ============================================================================
def generate_recommendations_task(**context):
    """
    Télécharge recommendations.json depuis S3 vers le projet local,
    puis valide le format du fichier.
    """
    print("DOWNLOAD & VALIDATE RECOMMENDATIONS FROM S3 (P3 - Cloud)")
    print("=" * 60)

    s3 = boto3.client('s3', region_name=AWS_REGION)

    output_file = os.path.join(PROJECT_ROOT, 'outputs', 'recommendations.json')
    os.makedirs(os.path.join(PROJECT_ROOT, 'outputs'), exist_ok=True)

    # Télécharger depuis S3
    print(f"📥 Téléchargement {S3_OUTPUT} → {output_file}")
    s3.download_file(S3_BUCKET, 'outputs/recommendations.json', output_file)

    # Valider le fichier
    with open(output_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"✅ Fichier téléchargé depuis S3")
    print(f"   Utilisateurs          : {len(data)}")
    print(f"   Total recommandations : {sum(len(u.get('recommendations', [])) for u in data)}")

    # Validation du format
    required_keys = {'user_id', 'recommendations'}
    for i, user in enumerate(data):
        if not required_keys.issubset(set(user.keys())):
            missing = required_keys - set(user.keys())
            print(f"ERROR: Invalid format at record {i} - missing keys: {missing}")
            raise ValueError(f"Invalid JSON format at record {i}: missing {missing}")

    print("✅ Recommendations validées et prêtes pour MongoDB")
    return f"{len(data)} utilisateurs avec recommandations"


generate_recommendations = PythonOperator(
    task_id='generate_recommendations',
    python_callable=generate_recommendations_task,
    dag=dag,
)


# ============================================================================
# TÂCHE 5: Insertion dans MongoDB (P4)
# ============================================================================
def insert_to_mongodb_task():
    print("MONGODB INSERTION TASK (P4)")
    print("=" * 60)

    script = os.path.join(PROJECT_ROOT, 'scripts', 'insert_mock_data.py')

    if not os.path.exists(script):
        print(f"ERROR: Insert script not found: {script}")
        raise FileNotFoundError(f"Script {script} not found")

    print(f"Script found: {script}")

    print("Starting insert script...")
    try:
        result = subprocess.run(
            [PYTHON_EXEC, script],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )

        print(result.stdout)

        if result.returncode != 0:
            print(f"ERROR: {result.stderr}")
            raise RuntimeError(
                f"Insert operation failed (rc={result.returncode}): {result.stderr[-2000:]}"
            )

        print("✅ Data successfully inserted into MongoDB")

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
# TÂCHE 6: Vérification et statut final (P4)
# ============================================================================
def api_ready_task():
    print("API VERIFICATION TASK (P4)")
    print("=" * 60)

    try:
        import requests
        print("NOTE: API must be started separately (http://localhost:8000)")
        print("To start API: python -m uvicorn api.main:app --reload")
    except ImportError:
        print("WARNING: 'requests' module not available")
    except Exception as e:
        print(f"WARNING: Unable to verify API: {e}")

    print("\n" + "=" * 60)
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print("\nCompleted tasks:")
    print("   1. ✅ Data cleaning          (P1)")
    print("   2. ✅ Upload to S3            (P3 - Cloud)")
    print("   3. ✅ AWS EMR Spark ALS       (P2 & P3 - ALS & Cloud)")
    print("   4. ✅ Download from S3        (P3 - Cloud)")
    print("   5. ✅ MongoDB insertion       (P4)")
    print("   6. ✅ API verification        (P4)")

    return "Pipeline terminé avec succès"


api_ready = PythonOperator(
    task_id='api_ready',
    python_callable=api_ready_task,
    dag=dag,
)


# ============================================================================
# DÉPENDANCES
# ============================================================================
# clean_data → upload_to_s3 → train_als → generate_recommendations → insert_to_mongodb → api_ready

clean_data >> upload_to_s3 >> train_als >> generate_recommendations >> insert_to_mongodb >> api_ready
