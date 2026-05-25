# Commandes AWS — Personne 3 Cloud / EMR
## Auteur : Personne 3
## Projet : Moteur de recommandation e-commerce Big Data

---

## 1. Configuration AWS CLI
aws configure
# Access Key ID     : [votre clé]
# Secret Access Key : [votre clé]
# Region            : eu-west-1
# Output format     : json

## 2. Bucket S3
# Créer le bucket
aws s3 mb s3://ecommerce-bigdata-bucket --region eu-west-1

# Vérifier la structure
aws s3 ls s3://ecommerce-bigdata-bucket/

## 3. Upload fichiers
aws s3 cp data/ratings_clean.csv s3://ecommerce-bigdata-bucket/raw/
aws s3 cp scripts/train_als.py s3://ecommerce-bigdata-bucket/scripts/

## 4. Rôles IAM
aws emr create-default-roles

## 5. Cluster EMR
aws emr create-cluster \
    --name "ecommerce-cluster" \
    --release-label emr-6.9.0 \
    --applications Name=Spark \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
        InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 \
    --use-default-roles \
    --region eu-west-1 \
    --auto-terminate

## 6. Suivre le job
aws emr describe-cluster \
    --cluster-id j-XXXXXXXXXX \
    --region eu-west-1 \
    --query "Cluster.Status.State"

## 7. Récupérer l'output
aws s3 cp s3://ecommerce-bigdata-bucket/outputs/recommendations.json outputs/

---

## Architecture Cloud

Dataset (ratings_clean.csv)
        ↓
S3 Bucket (raw/)
        ↓
EMR Cluster (Spark ALS)
        ↓
S3 Bucket (outputs/)
        ↓
recommendations.json → MongoDB (P4)

---

## Coût estimé
| Service  | Usage         | Coût    |
|----------|---------------|---------|
| S3       | < 1 Go        | 0.00$   |
| EMR      | ~20 min       | ~0.50$  |
| Total    |               | ~0.50$  |