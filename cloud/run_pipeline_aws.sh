#!/bin/bash
# run_pipeline_aws.sh — Pipeline complet AWS EMR
# Auteur : Personne 3 — Cloud / EMR
# Projet : Moteur de recommandation e-commerce

echo "🔧 Création des rôles IAM..."
aws emr create-default-roles

echo "🚀 Lancement cluster EMR + job Spark ALS..."
CLUSTER_ID=$(aws emr create-cluster \
    --name "ecommerce-cluster" \
    --release-label emr-6.9.0 \
    --applications Name=Spark \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
        InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 \
    --use-default-roles \
    --region eu-west-1 \
    --auto-terminate \
    --steps \
        Type=Spark,\
        Name="ALS Job",\
        ActionOnFailure=CONTINUE,\
        Args=[s3://ecommerce-bigdata-bucket/scripts/train_als.py,\
--input,s3://ecommerce-bigdata-bucket/raw/ratings_clean.csv,\
--output,s3://ecommerce-bigdata-bucket/outputs/] \
    --query 'ClusterId' \
    --output text)

echo "📋 Cluster ID : $CLUSTER_ID"
echo "⏳ Attente de la fin du job..."

aws emr wait cluster-terminated \
    --cluster-id $CLUSTER_ID \
    --region eu-west-1

echo "📥 Téléchargement recommendations.json..."
aws s3 cp s3://ecommerce-bigdata-bucket/outputs/recommendations.json outputs/

echo "🎉 Pipeline terminé ! recommendations.json disponible dans outputs/"