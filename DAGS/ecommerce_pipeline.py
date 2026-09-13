from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
)
def ecommerce_pipeline():


    # =========================================
    # 1. EXTRACT DATA FROM API
    # =========================================

    @task
    def extract_api():

        import requests
        import json

        url = "https://dummyjson.com/products"

        response = requests.get(
            url,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        output_path = (
            "/home/moham/airflow_project/"
            "raw_products.json"
        )

        with open(output_path, "w") as f:
            json.dump(data, f)

        print(
            "API extraction completed successfully"
        )

        return output_path


    # =========================================
    # 2. CLEAN DATA WITH PANDAS
    # =========================================

    @task
    def pandas_clean():

        import pandas as pd
        import json

        input_path = (
            "/home/moham/airflow_project/"
            "raw_products.json"
        )

        output_path = (
            "/home/moham/airflow_project/"
            "clean_products_airflow.csv"
        )

        with open(input_path, "r") as f:
            data = json.load(f)

        df = pd.DataFrame(
            data["products"]
        )

        # Fill missing brands
        df["brand"] = (
            df["brand"]
            .fillna("unknown")
        )

        # Keep required columns
        df = df[
            [
                "id",
                "title",
                "category",
                "price",
                "discountPercentage",
                "rating",
                "stock",
                "brand"
            ]
        ]

        # Calculate final selling price
        df["final_price"] = (
            df["price"]
            -
            (
                df["price"]
                *
                df["discountPercentage"]
                / 100
            )
        ).round(2)

        # Remove duplicates
        df = df.drop_duplicates(
            subset=["id"]
        )

        # Save cleaned CSV
        df.to_csv(
            output_path,
            index=False
        )

        print(
            "Pandas cleaning completed successfully"
        )

        print(
            "Rows:",
            len(df)
        )

        return output_path


    # =========================================
    # 3. UPLOAD CLEAN DATA TO AWS S3
    # =========================================

    @task
    def upload_to_s3():

        import boto3

        file_path = (
            "/home/moham/airflow_project/"
            "clean_products_airflow.csv"
        )

        bucket_name = (
            "nazeer-ecommerce-data-2026"
        )

        s3_key = (
            "clean/"
            "clean_products_airflow.csv"
        )

        s3 = boto3.client("s3")

        s3.upload_file(
            file_path,
            bucket_name,
            s3_key
        )

        print(
            "S3 upload completed successfully"
        )


    # =========================================
    # 4. LOAD DATA INTO POSTGRESQL
    # =========================================

    @task
    def load_postgres():

        import pandas as pd
        import os

        from sqlalchemy import (
            create_engine,
            text
        )

        file_path = (
            "/home/moham/airflow_project/"
            "clean_products_airflow.csv"
        )

        postgres_url = os.getenv(
            "POSTGRES_URL"
        )

        if not postgres_url:
            raise ValueError(
                "POSTGRES_URL environment "
                "variable is not set"
            )

        df = pd.read_csv(
            file_path
        )

        engine = create_engine(
            postgres_url
        )

        with engine.begin() as conn:

            conn.execute(
                text("DELETE FROM products")
            )

            df.to_sql(
                "products",
                conn,
                if_exists="append",
                index=False
            )

        print(
            "PostgreSQL load completed successfully"
        )

    # =========================================
    # 5. PYSPARK TRANSFORMATION
    # =========================================

    @task
    def pyspark_transform():

        from pyspark.sql import (
            SparkSession
        )

        from pyspark.sql.functions import (
            col,
            round
        )

        spark = (
            SparkSession.builder
            .appName(
                "EcommerceAirflow"
            )
            .getOrCreate()
        )

        input_path = (
            "/home/moham/airflow_project/"
            "clean_products_airflow.csv"
        )

        output_path = (
            "/home/moham/airflow_project/"
            "airflow_pyspark_output"
        )

        df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True
        )

        # Extra Spark transformation
        df = df.withColumn(
            "discount_amount",
            round(
                col("price")
                -
                col("final_price"),
                2
            )
        )

        # Remove duplicate IDs
        df = df.dropDuplicates(
            ["id"]
        )

        # Save as Parquet
        (
            df.write
            .mode("overwrite")
            .parquet(
                output_path
            )
        )

        print(
            "PySpark transformation "
            "completed successfully"
        )

        spark.stop() 


    # =========================================
    # 6. DBT MODEL + TEST
    # =========================================

    @task
    def run_dbt():

        import subprocess

        dbt_project_dir = (
            "/home/moham/airflow_project/"
            "ecommerce_dbt"
        )

        subprocess.run(
            ["dbt", "run"],
            cwd=dbt_project_dir,
            check=True
        )

        subprocess.run(
            ["dbt", "test"],
            cwd=dbt_project_dir,
            check=True
        )

        print(
            "dbt run and tests "
            "completed successfully"
        )


    # =========================================
    # PIPELINE ORDER
    # =========================================

    extract_task = extract_api()

    clean_task = pandas_clean()

    s3_task = upload_to_s3()

    postgres_task = load_postgres()

    spark_task = pyspark_transform()

    dbt_task = run_dbt()


    (
        extract_task
        >> clean_task
        >> s3_task
        >> postgres_task
        >> spark_task
        >> dbt_task
    )


ecommerce_pipeline()
