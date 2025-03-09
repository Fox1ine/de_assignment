import os
from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from logger_config import setup_logger

logger = setup_logger('bets_loader', 'bets_loader.log')


def load_env_variables():
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


def connect_to_postgres(db_config):
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=db_config["db_host"],
            port=db_config["db_port"],
            dbname=db_config["db_name"],
            user=db_config["db_user"],
            password=db_config["db_password"]
        )
        logger.info("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def create_schema(conn, sql_file_path, schema_type="bets"):
    """Create the database schema from an SQL file."""
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    with conn.cursor() as cur:
        cur.execute(sql_script)
    conn.commit()
    logger.info(f"{schema_type.capitalize()} schema created successfully using {sql_file_path}.")


def create_spark_session(jdbc_driver_path):
    """Initialize a SparkSession with JDBC driver configuration."""
    try:
        spark = SparkSession.builder \
            .appName("Load Bets to PostgreSQL") \
            .config("spark.jars", jdbc_driver_path) \
            .config("spark.driver.extraClassPath", jdbc_driver_path) \
            .config("spark.executor.extraClassPath", jdbc_driver_path) \
            .getOrCreate()
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}")
        raise


def read_bets_csv(spark, csv_path):
    """Read and process bets data from a CSV file."""
    try:
        bets_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)

        # Ensure 'row_id' column exists for unique identification
        if "row_id" not in bets_df.columns:
            bets_df = bets_df.withColumn("row_id", monotonically_increasing_id())
        return bets_df
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise


def get_db_columns(conn, table_name="bets"):
    """Retrieve column names from the specified PostgreSQL table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        db_columns = {row[0] for row in cur.fetchall()}
    logger.info(f"Columns in PostgreSQL for table {table_name}: {db_columns}")
    return db_columns


def prepare_dataframe(bets_df, db_columns):
    """Prepare the DataFrame by selecting only necessary columns and repartitioning."""
    if "id" in db_columns and "row_id" in bets_df.columns:
        bets_df = bets_df.drop("row_id")
    bets_df = bets_df.select([col(c) for c in db_columns if c in bets_df.columns])
    bets_df = bets_df.repartition(10)  # Optimize performance with partitioning
    logger.info(f"Final DataFrame schema: {bets_df.schema}")
    return bets_df


def write_dataframe_to_db(bets_df, db_config):
    """Write the prepared DataFrame to the PostgreSQL database."""
    postgres_url = f"jdbc:postgresql://{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}"
    postgres_properties = {
        "user": db_config["db_user"],
        "password": db_config["db_password"],
        "driver": "org.postgresql.Driver"
    }
    try:
        bets_df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "bets") \
            .option("user", postgres_properties["user"]) \
            .option("password", postgres_properties["password"]) \
            .option("driver", postgres_properties["driver"]) \
            .option("batchsize", 10000) \
            .mode("append") \
            .save()
        logger.info("Data successfully written to 'bets' table.")
    except Exception as e:
        logger.error(f"Error writing data to PostgreSQL: {e}")
        raise


def load_bets_data():
    """Main function to orchestrate the bets data loading process."""
    db_config = load_env_variables()

    with connect_to_postgres(db_config) as conn:
        create_schema(conn, db_config["sql_bets_file_path"], schema_type="bets")

    spark = create_spark_session(db_config["jdbc_driver_path"])
    bets_df = read_bets_csv(spark, db_config["csv_path"])

    with connect_to_postgres(db_config) as conn:
        db_columns = get_db_columns(conn, table_name="bets")

    bets_df = prepare_dataframe(bets_df, db_columns)
    write_dataframe_to_db(bets_df, db_config)

    spark.stop()

    with connect_to_postgres(db_config) as conn:
        create_schema(conn, db_config["sql_bets_indexes_path"], schema_type="bets indexes")

    logger.info("Bets data load process completed successfully.")


if __name__ == "__main__":
    load_bets_data()
