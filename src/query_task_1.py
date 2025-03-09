import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, min as spark_min, broadcast, lit
from pyspark.sql.window import Window

def get_spark_session():
    """Initialize and return a SparkSession with JDBC config."""
    return SparkSession.builder \
        .appName("Bets Query") \
        .config("spark.jars", os.getenv("JDBC_DRIVER_PATH", "/opt/bitnami/spark/postgresql-42.5.0.jar")) \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.ui.enabled", "true") \
        .getOrCreate()

def load_data_from_postgres(spark):
    """Load bets and events data from PostgreSQL into Spark DataFrames."""
    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'postgres_db')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'betsdb')}"
    db_properties = {
        "user": os.getenv("DB_USER", "user"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "driver": "org.postgresql.Driver"
    }

    bets_df = spark.read.jdbc(url=db_url, table="bets", properties=db_properties)
    events_df = spark.read.jdbc(url=db_url, table="events", properties=db_properties)

    return bets_df, events_df

def run_query_with_pyspark(spark):
    """Run the filtering and aggregation logic with PySpark based on task conditions."""
    bets_df, events_df = load_data_from_postgres(spark)

    events_df = broadcast(events_df)  # Improve join performance

    join_df = bets_df.join(events_df, "event_id", "inner").cache()

    filtered_df = join_df.filter(
        (col("create_time") >= "2022-03-14 12:00:00") &
        (col("event_stage") == "Prematch") &
        (col("amount") >= 10) &
        (col("settlement_time") <= "2022-03-15 12:00:00") &
        (col("bet_type") != "System") &
        (~col("result").isin("Cashout", "Return", "TechnicalReturn")) &
        (col("is_free_bet") == False)
    )

    # Aggregate by bet_id and player_id
    aggregated_df = filtered_df.groupBy("bet_id", "player_id").agg(
        count("*").alias("total_events"),
        count(when(col("sport") == "E-Sports", True)).alias("esports_events"),
        spark_min("accepted_odd").alias("min_odd")
    )

    # Filter bets that are fully E-Sports and have all odds >= 1.5
    result_df = aggregated_df.filter(
        (col("total_events") == col("esports_events")) &
        (col("min_odd") >= 1.5)
    ).select("player_id").distinct()

    result_df.explain(True)
    result_df.show(truncate=False, vertical=True)
    return result_df

def run_sql_file_query(spark):
    """Execute the same logic as SQL script if defined in external file."""
    sql_file_path = os.getenv("SQL_FIRST_TASK_PATH", "/mnt/sql/query_task_1.sql")

    try:
        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_query = file.read()
    except Exception as e:
        raise Exception(f"Unable to read SQL file {sql_file_path}: {e}")

    bets_df, events_df = load_data_from_postgres(spark)
    bets_df.createOrReplaceTempView("bets")
    events_df.createOrReplaceTempView("events")

    result_df = spark.sql(sql_query)
    return result_df

if __name__ == "__main__":
    spark = get_spark_session()

    # Run query via PySpark API
    result_df = run_query_with_pyspark(spark)
    result_df.show()

    # Optionally, run the same query via external SQL file
    sql_result_df = run_sql_file_query(spark)
    sql_result_df.show()

    spark.stop()