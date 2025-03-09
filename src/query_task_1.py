from typing import Dict
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from logger_config import setup_logger

logger = setup_logger('query_task_1', 'query_task_1.log')

def load_env_variables() -> Dict[str, str]:
    """Load environment variables from .env file."""
    load_dotenv()
    return {
        "db_host": os.getenv("DB_HOST"),
        "db_port": os.getenv("DB_PORT"),
        "db_name": os.getenv("DB_NAME"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "jdbc_driver_path": os.getenv("JDBC_DRIVER_PATH"),
        "sql_task_1_path": os.getenv("SQL_TASK_1_FILE_PATH")
    }

def create_spark_session(jdbc_driver_path: str) -> SparkSession:
    """Create SparkSession with PostgreSQL JDBC driver."""
    try:
        spark = SparkSession.builder \
            .appName("Query Task 1") \
            .config("spark.jars", jdbc_driver_path) \
            .config("spark.driver.extraClassPath", jdbc_driver_path) \
            .config("spark.executor.extraClassPath", jdbc_driver_path) \
            .getOrCreate()
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}")
        raise

def load_table(spark: SparkSession, table_name: str, db_config: Dict[str, str]) -> DataFrame:
    """Load table from PostgreSQL as Spark DataFrame."""
    jdbc_url = f"jdbc:postgresql://{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}"
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", db_config["db_user"]) \
        .option("password", db_config["db_password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def run_query_with_pyspark(bets_df: DataFrame, events_df: DataFrame) -> None:
    """Perform the filtering and aggregation logic using PySpark."""
    filtered_bets = bets_df.filter(
        (bets_df.create_time >= '2022-03-14 12:00:00') &
        (bets_df.event_stage == 'Prematch') &
        (bets_df.amount >= 10) &
        (bets_df.settlement_time <= '2022-03-15 12:00:00') &
        (bets_df.bet_type != 'System') &
        (~bets_df.result.isin('Cashout', 'Return', 'TechnicalReturn')) &
        (bets_df.is_free_bet == False)
    )

    joined = filtered_bets.join(events_df, on="event_id")

    from pyspark.sql.functions import col, count, when, min as spark_min
    agg = joined.groupBy("bet_id", "player_id").agg(
        count("event_id").alias("total_events"),
        count(when(col("sport") == "E-Sports", True)).alias("esports_events"),
        spark_min("accepted_odd").alias("min_odd")
    )

    result = agg.filter(
        (col("total_events") == col("esports_events")) &
        (col("min_odd") >= 1.5)
    ).select("player_id").distinct()

    logger.info("Players matching criteria:")
    result.show(truncate=False)

def run_sql_file_query(spark: SparkSession, sql_path: str) -> None:
    """Run the query from SQL file using Spark SQL."""
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        spark.sql("USE bets")  # assuming the database context is needed
        result = spark.sql(sql)
        logger.info("Query from SQL file executed successfully:")
        result.show(truncate=False)
    except Exception as e:
        logger.error(f"Error executing SQL file: {e}")
        raise

def main() -> None:
    """Main function to run query task 1."""
    db_config = load_env_variables()
    spark = create_spark_session(db_config["jdbc_driver_path"])

    bets_df = load_table(spark, "bets", db_config)
    events_df = load_table(spark, "events", db_config)

    run_query_with_pyspark(bets_df, events_df)
    run_sql_file_query(spark, db_config["sql_task_1_path"])

    spark.stop()

if __name__ == "__main__":
    main()