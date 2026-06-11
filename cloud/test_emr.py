import boto3
import time

AWS_REGION = 'eu-west-1'
S3_BUCKET  = 'ecommerce-bigdata-bucket'

emr = boto3.client('emr', region_name=AWS_REGION)
s3  = boto3.client('s3',  region_name=AWS_REGION)

# Test 1 — Vérifier que S3 est accessible
print("Test 1 — Vérification S3...")
response = s3.list_objects_v2(Bucket=S3_BUCKET)
files = [obj['Key'] for obj in response.get('Contents', [])]
print(f"✅ Fichiers dans S3 : {files}")

# Test 2 — Vérifier que les fichiers nécessaires sont là
print("\nTest 2 — Fichiers nécessaires...")
required = [
    'raw/ratings_clean.csv',
    'scripts/train_als.py',
]
for f in required:
    exists = any(obj['Key'] == f for obj in response.get('Contents', []))
    print(f"{'✅' if exists else '❌'} {f}")

# Test 3 — Lancer le cluster EMR
print("\nTest 3 — Lancement cluster EMR...")
response = emr.run_job_flow(
    Name='ecommerce-cluster-test',
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
        'KeepJobFlowAliveWhenNoSteps': False,
    },
    Steps=[
        {
            'Name': 'ALS Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    f's3://{S3_BUCKET}/scripts/train_als.py',
                    '--input',        f's3://{S3_BUCKET}/raw/ratings_clean.csv',
                    '--output',       f's3://{S3_BUCKET}/outputs/recommendations.json',
                    '--model-output', f's3://{S3_BUCKET}/models/als_model',
                    '--top-n',        '5',
                ],
            },
        }
    ],
    LogUri=f's3://{S3_BUCKET}/logs/',
    ServiceRole='EMR_DefaultRole',
    JobFlowRole='EMR_EC2_DefaultRole',
    VisibleToAllUsers=True,
)

cluster_id = response['JobFlowId']
print(f"✅ Cluster lancé : {cluster_id}")

# Test 4 — Attendre la fin
print("\nTest 4 — Attente fin du job...")
while True:
    state = emr.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']
    print(f"   État : {state}")
    if state == 'TERMINATED':
        print("✅ Job terminé avec succès !")
        break
    elif state == 'TERMINATED_WITH_ERRORS':
        print("❌ Job terminé avec erreurs !")
        break
    else:
        time.sleep(60)

# Test 5 — Vérifier recommendations.json dans S3
print("\nTest 5 — Vérification output S3...")
try:
    s3.head_object(Bucket=S3_BUCKET, Key='outputs/recommendations.json')
    print("✅ recommendations.json présent dans S3 !")
    
    # Télécharger et afficher les 2 premières lignes
    import json
    obj = s3.get_object(Bucket=S3_BUCKET, Key='outputs/recommendations.json')
    data = json.loads(obj['Body'].read())
    print(f"   Utilisateurs : {len(data)}")
    print(f"   Exemple : {data[0]}")
except Exception as e:
    print(f"❌ recommendations.json absent : {e}")