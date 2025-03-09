from typing import Dict, Set
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col
from data_utils import connect_to_postgres, create_schema, create_spark_session, get_db_columns
from logger_config import setup_logger

logger = setup_logger('bets_loader', 'bets_loader.log')

def load_env_variables() -> Dict[str, str]:
    """Load environment variables from .env file."""
    load_dotenv()
    return {
        "db_host": os.getenv("DB_HOST"),
        "db_port": os.getenv("DB_PORT"),
        "db_name": os.getenv("DB_NAME"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "sql_bets_file_path": os.getenv("SQL_BETS_FILE_PATH"),
        "sql_bets_indexes_path": os.getenv("SQL_BETS_INDEXES_PATH"),
        "csv_path": os.getenv("CSV_BETS_FILE_PATH"),
        "jdbc_driver_path": os.getenv("JDBC_DRIVER_PATH")
    }

def read_bets_csv(spark: SparkSession, csv_path: str) -> DataFrame:
    """Read bets CSV file into a Spark DataFrame. Add unique row_id if not present."""
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    if "row_id" not in df.columns:
        df = df.withColumn("row_id", monotonically_increasing_id())
    return df

def prepare_dataframe(df: DataFrame, db_columns: Set[str]) -> DataFrame:
    """Drop temporary row_id if necessary, select valid DB columns, and repartition for parallelism."""
    if "id" in db_columns and "row_id" in df.columns:
        df = df.drop("row_id")
    return df.select([col(c) for c in db_columns if c in df.columns]).repartition(10)

def write_dataframe_to_db(df: DataFrame, db_config: Dict[str, str], table: str) -> None:
    """Write Spark DataFrame to PostgreSQL using JDBC."""
    df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}") \
        .option("dbtable", table) \
        .option("user", db_config["db_user"]) \
        .option("password", db_config["db_password"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 10000) \
        .mode("append") \
        .save()
    logger.info(f"Data successfully written to '{table}' table.")

def load_bets_data() -> None:
    """Main function that coordinates reading, transforming, and writing bets data."""
    config = load_env_variables()

    # Create schema before loading data
    with connect_to_postgres(config) as conn:
        create_schema(conn, config["sql_bets_file_path"], name="bets")

    spark = create_spark_session(config["jdbc_driver_path"])
    df = read_bets_csv(spark, config["csv_path"])

    # Get expected column names from DB
    with connect_to_postgres(config) as conn:
        columns = get_db_columns(conn, "bets")

    df = prepare_dataframe(df, columns)
    write_dataframe_to_db(df, config, "bets")
    spark.stop()

    # Create indexes after loading
    with connect_to_postgres(config) as conn:
        create_schema(conn, config["sql_bets_indexes_path"], name="bets indexes")

if __name__ == "__main__":
    load_bets_data()