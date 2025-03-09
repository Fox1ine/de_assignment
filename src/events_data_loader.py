import os
from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from logger_config import setup_logger

logger = setup_logger('events_loader', 'events_loader.log')


def load_env_variables():
    """Load environment variables from the .env file."""
    load_dotenv()
    return {
        "db_host": os.getenv("DB_HOST"),
        "db_port": os.getenv("DB_PORT"),
        "db_name": os.getenv("DB_NAME"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "sql_events_file_path": os.getenv("SQL_EVENTS_FILE_PATH"),
        "sql_events_indexes_path": os.getenv("SQL_EVENTS_INDEXES_PATH"),
        "csv_path": os.getenv("CSV_EVENTS_FILE_PATH"),
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


def create_schema(conn, sql_file_path):
    """Execute an SQL script to create the database schema."""
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    with conn.cursor() as cur:
        cur.execute(sql_script)
    conn.commit()
    logger.info(f"Schema created successfully using {sql_file_path}.")


def create_spark_session(jdbc_driver_path):
    """Initialize a SparkSession for processing event data."""
    try:
        spark = SparkSession.builder \
            .appName("Load Events to PostgreSQL") \
            .config("spark.jars", jdbc_driver_path) \
            .config("spark.driver.extraClassPath", jdbc_driver_path) \
            .config("spark.executor.extraClassPath", jdbc_driver_path) \
            .getOrCreate()
        logger.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}")
        raise


def read_events_csv(spark, csv_path):
    """Read and process event data from a CSV file."""
    try:
        events_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(csv_path)
        logger.info("CSV file read successfully.")

        # Ensure unique row identification if missing
        if "row_id" not in events_df.columns:
            events_df = events_df.withColumn("row_id", monotonically_increasing_id())

        return events_df
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise


def get_db_columns(conn, table_name='events'):
    """Retrieve column names from the PostgreSQL table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        db_columns = {row[0] for row in cur.fetchall()}
    return db_columns


def prepare_dataframe(events_df, db_columns):
    """Select only required columns and optimize DataFrame partitions."""
    if "id" in db_columns and "row_id" in events_df.columns:
        events_df = events_df.drop("row_id")
    events_df = events_df.select([col(c) for c in db_columns if c in events_df.columns])
    events_df = events_df.repartition(10)  # Improve parallel processing
    logger.info(f"Final DataFrame schema: {events_df.schema}")
    return events_df


def write_dataframe_to_db(events_df, db_config):
    """Write the processed DataFrame to the PostgreSQL database."""
    postgres_url = f"jdbc:postgresql://{db_config['db_host']}:{db_config['db_port']}/{db_config['db_name']}"
    postgres_properties = {
        "user": db_config["db_user"],
        "password": db_config["db_password"],
        "driver": "org.postgresql.Driver"
    }
    try:
        events_df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", "events") \
            .option("user", postgres_properties["user"]) \
            .option("password", postgres_properties["password"]) \
            .option("driver", postgres_properties["driver"]) \
            .option("batchsize", 10000) \
            .mode("append") \
            .save()
        logger.info("Data successfully written to 'events' table.")
    except Exception as e:
        logger.error(f"Error writing data to PostgreSQL: {e}")
        raise


def load_events_data():
    """Main function to orchestrate event data processing."""
    db_config = load_env_variables()

    with connect_to_postgres(db_config) as conn:
        create_schema(conn, db_config["sql_events_file_path"])

    spark = create_spark_session(db_config["jdbc_driver_path"])
    events_df = read_events_csv(spark, db_config["csv_path"])

    with connect_to_postgres(db_config) as conn:
        db_columns = get_db_columns(conn)

    events_df = prepare_dataframe(events_df, db_columns)
    write_dataframe_to_db(events_df, db_config)

    spark.stop()

    with connect_to_postgres(db_config) as conn:
        create_schema(conn, db_config["sql_events_indexes_path"])
    logger.info("Events data load process completed successfully.")


if __name__ == "__main__":
    load_events_data()