import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructField, StructType, StringType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Key space created!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")

    id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    address = kwargs.get("address")
    post_code = kwargs.get("post_code")
    email = kwargs.get("email")
    username = kwargs.get("username")
    dob = kwargs.get("dob")
    registered_date = kwargs.get("registered_date")
    phone = kwargs.get("phone")
    picture = kwargs.get("picture")

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users (id, first_name, last_name, gender, address,
                    post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture))

    except Exception as e:
        logging.error('Failed to insert data due to {}'.format(e))


def create_spark_connection():
    spark_conn = None

    try:
        spark_conn = (SparkSession.builder
            .appName("SparkCassandraStreaming")
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2')
            .config('spark.cassandra.connection.host', 'localhost')
            .getOrCreate())
        
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    
    except Exception as e:
        logging.error("Failed to create spark connection due to {}".format(e))
    
    return spark_conn


def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = (spark_conn.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'localhost:9092')
            .option('subscribe', 'user_created')
            .option('startingOffsets', 'earliest')
            .load())

        logging.info("Spark dataframe created successfully!")
    
    except Exception as e:
        logging.error("Failed to create spark dataframe due to {}".format(e))

    return spark_df


def spark_dataframe_selection_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    df = (spark_df.selectExpr("CAST(value AS STRING)")
          .select(from_json(col('value'), schema).alias('data')).select("data.*"))

    return df


def create_cassandra_connection():
    cass_conn = None

    try:
        cluster = Cluster(['localhost'])
        cass_conn = cluster.connect()

    except Exception as e:
        logging.error("Failed to create cassandra connection due to {}".format(e))

    return cass_conn


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka
        spark_df = connect_to_kafka(spark_conn)
        df = spark_dataframe_selection_from_kafka(spark_df)

        # create cassandra connection
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Stream is being started ...")

            # stream_query = (df.writeStream.format("org.apache.spark.sql.cassandra")
            #                 .option('checkpointLocation', '/tmp/checkpoint')
            #                 .option('keyspace', 'spark_streams')
            #                 .option('table', 'created_users')
            #                 .start())
            
            stream_query = (df.writeStream.format("console")
                            .outputMode("append")
                            .start())
            
            stream_query.awaitTermination()

