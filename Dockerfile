FROM bitnami/spark:latest

RUN pip install python-dotenv psycopg2-binary

COPY postgresql-42.5.0.jar /opt/bitnami/spark/
COPY src/bets_data_loader.py /opt/bitnami/spark/
COPY src/events_data_loader.py /opt/bitnami/spark/
COPY spark-defaults.conf /opt/bitnami/spark/conf/
COPY src/logger_config.py /opt/bitnami/spark/
COPY requirements.txt /tmp/requirements.txt

ENV JDBC_DRIVER_PATH="/opt/bitnami/spark/postgresql-42.5.0.jar"

ENV SPARK_MODE=master

CMD ["/opt/bitnami/scripts/spark/run.sh"]
