#!/bin/bash
# setup_aws.sh — Configuration infrastructure AWS
# Auteur : Personne 3 — Cloud / EMR
# Projet : Moteur de recommandation e-commerce

echo "🪣 Création du bucket S3..."
aws s3 mb s3://ecommerce-bigdata-bucket --region eu-west-1

echo "📁 Création de la structure..."
aws s3api put-object --bucket ecommerce-bigdata-bucket --key raw/
aws s3api put-object --bucket ecommerce-bigdata-bucket --key processed/
aws s3api put-object --bucket ecommerce-bigdata-bucket --key outputs/
aws s3api put-object --bucket ecommerce-bigdata-bucket --key scripts/
aws s3api put-object --bucket ecommerce-bigdata-bucket --key models/

echo "📤 Upload des fichiers..."
aws s3 cp data/ratings_clean.csv s3://ecommerce-bigdata-bucket/raw/
aws s3 cp scripts/train_als.py s3://ecommerce-bigdata-bucket/scripts/

echo "✅ Infrastructure prête !"