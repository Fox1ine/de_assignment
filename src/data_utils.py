from typing import Dict, Set
import psycopg2
from psycopg2.extensions import connection as PGConnection
from pyspark.sql import SparkSession

def connect_to_postgres(config: Dict[str, str]) -> PGConnection:
    return psycopg2.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname=config["db_name"],
        user=config["db_user"],
        password=config["db_password"]
    )

def create_schema(conn: PGConnection, sql_path: str, name: str) -> None:
    with open(sql_path, 'r', encoding='utf-8') as f:
        script = f.read()
    with conn.cursor() as cur:
        cur.execute(script)
    conn.commit()

def create_spark_session(driver_path: str, app_name: str = "SparkApp") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", driver_path) \
        .config("spark.driver.extraClassPath", driver_path) \
        .config("spark.executor.extraClassPath", driver_path) \
        .getOrCreate()

def get_db_columns(conn: PGConnection, table_name: str) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
        return {row[0] for row in cur.fetchall()}
