#!/usr/bin/env python3
"""
Entraine un modele de recommandation e-commerce avec Spark MLlib ALS.

Entree attendue :
  user_id,product_id,rating

Sortie principale :
  outputs/recommendations.json
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REQUIRED_COLUMNS = {"user_id", "product_id", "rating"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train Spark MLlib ALS and export recommendations.json"
    )
    parser.add_argument(
        "--input",
        default="data/ratings_clean.csv",
        help="Chemin du CSV nettoye fourni par l'equipe Data.",
    )
    parser.add_argument(
        "--output",
        default="outputs/recommendations.json",
        help="Chemin du fichier JSON de recommandations a produire.",
    )
    parser.add_argument(
        "--model-output",
        default="models/als_model",
        help="Chemin ou sauvegarder le modele ALS.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Nombre de recommandations a produire par utilisateur.",
    )
    parser.add_argument("--rank", type=int, default=10, help="Dimension des facteurs latents.")
    parser.add_argument("--max-iter", type=int, default=10, help="Nombre d'iterations ALS.")
    parser.add_argument(
        "--reg-param",
        type=float,
        default=0.1,
        help="Regularisation du modele ALS.",
    )
    parser.add_argument(
        "--test-ratio",
        type=float,
        default=0.2,
        help="Part du dataset utilisee pour le test.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Graine aleatoire du split.")
    parser.add_argument(
        "--no-save-model",
        action="store_true",
        help="Ne sauvegarde pas le modele entraine.",
    )
    return parser.parse_args()


def is_remote_path(path: str) -> bool:
    return "://" in path


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("EcommerceRecommendationALS")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def normalize_and_validate_columns(df: DataFrame) -> DataFrame:
    """Nettoie les noms de colonnes et verifie le contrat d'entree."""
    normalized_df = df.select([F.col(c).alias(c.strip()) for c in df.columns])
    missing_columns = REQUIRED_COLUMNS - set(normalized_df.columns)

    if missing_columns:
        raise ValueError(
            "Colonnes manquantes dans le fichier d'entree : "
            + ", ".join(sorted(missing_columns))
        )

    return normalized_df.select("user_id", "product_id", "rating")


def load_and_prepare_ratings(spark: SparkSession, input_path: str) -> DataFrame:
    """Lit le CSV, supprime les lignes invalides et prepare les types pour ALS."""
    if not is_remote_path(input_path) and not Path(input_path).exists():
        raise FileNotFoundError(f"Fichier introuvable : {input_path}")

    raw_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(input_path)
    )
    raw_df = normalize_and_validate_columns(raw_df)
    raw_count = raw_df.count()

    typed_df = raw_df.select(
        F.trim(F.col("user_id").cast("string")).cast("int").alias("user_id"),
        F.trim(F.col("product_id").cast("string")).cast("int").alias("product_id"),
        F.trim(F.col("rating").cast("string")).cast("float").alias("rating"),
    )

    clean_df = typed_df.dropna(subset=["user_id", "product_id", "rating"])
    clean_count = clean_df.count()

    # Si une paire utilisateur-produit apparait plusieurs fois, on garde la note moyenne.
    ratings_df = clean_df.groupBy("user_id", "product_id").agg(
        F.avg("rating").cast("float").alias("rating")
    )
    final_count = ratings_df.count()

    if final_count == 0:
        raise ValueError("Aucune ligne valide apres nettoyage du fichier ratings.")

    logging.info("Lignes lues : %s", raw_count)
    logging.info("Lignes valides apres conversion : %s", clean_count)
    logging.info("Interactions finales apres aggregation : %s", final_count)

    return ratings_df


def build_als_estimator(args: argparse.Namespace) -> ALS:
    return ALS(
        rank=args.rank,
        maxIter=args.max_iter,
        regParam=args.reg_param,
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
    )


def train_als_model(ratings_df: DataFrame, args: argparse.Namespace):
    train_df, test_df = ratings_df.randomSplit(
        [1.0 - args.test_ratio, args.test_ratio], seed=args.seed
    )

    train_count = train_df.count()
    test_count = test_df.count()

    if train_count == 0:
        raise ValueError("Le jeu d'entrainement est vide. Ajoutez plus de ratings.")

    logging.info("Interactions train : %s", train_count)
    logging.info("Interactions test : %s", test_count)

    als = build_als_estimator(args)
    evaluation_model = als.fit(train_df)

    rmse: Optional[float] = None
    if test_count > 0:
        predictions = evaluation_model.transform(test_df)
        prediction_count = predictions.count()

        if prediction_count > 0:
            evaluator = RegressionEvaluator(
                metricName="rmse",
                labelCol="rating",
                predictionCol="prediction",
            )
            rmse = evaluator.evaluate(predictions)
            logging.info("RMSE sur le jeu de test : %.4f", rmse)
        else:
            logging.warning(
                "Evaluation impossible : toutes les predictions test ont ete "
                "supprimees par coldStartStrategy='drop'."
            )
    else:
        logging.warning("Evaluation ignoree : le split test est vide.")

    logging.info("Reentrainement du modele final sur toutes les interactions valides.")
    final_model = als.fit(ratings_df)

    return final_model, rmse


def build_recommendation_payload(recommendations_df: DataFrame) -> List[Dict[str, Any]]:
    """Convertit le DataFrame Spark en liste JSON imbriquee."""
    payload: List[Dict[str, Any]] = []

    for row in recommendations_df.orderBy("user_id").collect():
        user_recommendations = []

        for recommendation in row["recommendations"] or []:
            recommendation_dict = recommendation.asDict()
            predicted_score = recommendation_dict.get("score")

            user_recommendations.append(
                {
                    "product_id": int(recommendation_dict["product_id"]),
                    "score": round(float(predicted_score), 4),
                }
            )

        payload.append(
            {
                "user_id": int(row["user_id"]),
                "recommendations": user_recommendations,
            }
        )

    return payload


def get_recommendation_candidate_count(model, ratings_df: DataFrame, top_n: int) -> int:
    """Calcule combien de candidats ALS demander avant filtrage des produits vus."""
    item_count = model.itemFactors.count()
    max_seen_count = (
        ratings_df.groupBy("user_id")
        .count()
        .agg(F.max("count").alias("max_seen_count"))
        .first()["max_seen_count"]
        or 0
    )

    candidate_count = min(int(item_count), int(top_n + max_seen_count))

    logging.info("Produits appris par ALS : %s", item_count)
    logging.info("Interactions max par utilisateur : %s", max_seen_count)
    logging.info("Candidats ALS demandes par utilisateur : %s", candidate_count)

    return candidate_count


def generate_unseen_recommendations(model, ratings_df: DataFrame, top_n: int) -> DataFrame:
    """Genere les top N recommandations en excluant les produits deja notes."""
    candidate_count = get_recommendation_candidate_count(model, ratings_df, top_n)
    if candidate_count == 0:
        return ratings_df.select("user_id").distinct().withColumn(
            "recommendations",
            F.array().cast("array<struct<rank:int,product_id:int,score:float>>"),
        )

    candidates_df = model.recommendForAllUsers(candidate_count)
    exploded_candidates_df = candidates_df.select(
        "user_id",
        F.explode("recommendations").alias("recommendation"),
    ).select(
        "user_id",
        F.col("recommendation.product_id").alias("product_id"),
        F.col("recommendation.rating").alias("score"),
    )

    seen_products_df = ratings_df.select("user_id", "product_id").distinct()
    unseen_candidates_df = exploded_candidates_df.join(
        seen_products_df,
        on=["user_id", "product_id"],
        how="left_anti",
    )

    ranking_window = Window.partitionBy("user_id").orderBy(
        F.desc("score"),
        F.asc("product_id"),
    )
    ranked_recommendations_df = (
        unseen_candidates_df.withColumn("rank", F.row_number().over(ranking_window))
        .filter(F.col("rank") <= top_n)
        .select("user_id", "rank", "product_id", "score")
    )

    grouped_recommendations_df = ranked_recommendations_df.groupBy("user_id").agg(
        F.sort_array(
            F.collect_list(F.struct("rank", "product_id", "score"))
        ).alias("recommendations")
    )

    return ratings_df.select("user_id").distinct().join(
        grouped_recommendations_df,
        on="user_id",
        how="left",
    )


def write_text(spark: SparkSession, output_path: str, content: str) -> None:
    """Ecrit en local ou via Hadoop FS pour faciliter Dataproc/GCS."""
    if is_remote_path(output_path):
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(output_path)
        filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())

        if filesystem.exists(hadoop_path):
            filesystem.delete(hadoop_path, True)

        output_stream = filesystem.create(hadoop_path, True)
        try:
            output_stream.write(bytearray(content.encode("utf-8")))
        finally:
            output_stream.close()
        return

    local_path = Path(output_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(content, encoding="utf-8")


def save_recommendations_json(
    spark: SparkSession,
    model,
    ratings_df: DataFrame,
    output_path: str,
    top_n: int,
) -> int:
    recommendations_df = generate_unseen_recommendations(model, ratings_df, top_n)
    payload = build_recommendation_payload(recommendations_df)
    json_content = json.dumps(payload, ensure_ascii=False, indent=2)

    write_text(spark, output_path, json_content)
    logging.info("Fichier recommendations.json sauvegarde : %s", output_path)

    return len(payload)


def save_model(model, model_output: str) -> None:
    if not is_remote_path(model_output):
        Path(model_output).parent.mkdir(parents=True, exist_ok=True)

    model.write().overwrite().save(model_output)
    logging.info("Modele ALS sauvegarde : %s", model_output)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    if args.top_n <= 0:
        raise ValueError("--top-n doit etre superieur a 0.")
    if not 0.0 < args.test_ratio < 1.0:
        raise ValueError("--test-ratio doit etre entre 0 et 1.")

    spark = build_spark_session()

    try:
        ratings_df = load_and_prepare_ratings(spark, args.input)
        model, rmse = train_als_model(ratings_df, args)
        user_count = save_recommendations_json(
            spark,
            model,
            ratings_df,
            args.output,
            args.top_n,
        )

        if not args.no_save_model:
            save_model(model, args.model_output)

        logging.info("Nombre d'utilisateurs exportes : %s", user_count)
        if rmse is not None:
            logging.info("RMSE final : %.4f", rmse)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
