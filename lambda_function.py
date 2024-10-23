from pyspark.sql import SparkSession
import getpass
import json
import boto3
import os

from dotenv import load_dotenv

from services.data_source import (
    JSONDataSource,
    CSVDataSource,
    XMLDataSource,
    JDBCDataSource,
    ODBCDataSource,
    DatabaseConnector
)
from services.odbc_db_config import PostgreSQLConfig, MySQLConfig
from services.logger import ETLLogger, CloudWatchLogger
from services.data_source import DATASOURCE_MAP

from utils.helper_functions import fetch_connection_params, get_db_creds, fetch_model_mapping

load_dotenv()

CONNECTOR_DYNAMO_TABLE = os.getenv("CONNECTOR_DYNAMO_TABLE")
MODEL_MAPPING_DYNAMO_TABLE  = os.getenv("MODEL_MAPPING_DYNAMO_TABLE")

LOG_GROUP = os.getenv("LOG_GROUP")

DATABASE_CONFIG = {
    DATASOURCE_MAP["DB"]["MYSQL"]: {
        "jdbc_url": "jdbc:mysql://{host}:{port}/{database}",
        "driver": "com.mysql.cj.jdbc.Driver",
        "jar_path": "./jdbc-drivers/mysql-connector-java-8.0.28.jar"
    },
    DATASOURCE_MAP["DB"]["POSTGRES"]: {
        "jdbc_url": "jdbc:postgresql://{host}:{port}/{database}",
        "driver": "org.postgresql.Driver",
        "jar_path": "./jdbc-drivers/postgresql-42.7.4.jar"
    },
    DATASOURCE_MAP["DB"]["ORACLE"]: {
        "jdbc_url": "jdbc:oracle:thin:@{host}:{port}:{database}",
        "driver": "oracle.jdbc.driver.OracleDriver",
        "jar_path": "./jdbc-drivers/oracle-jdbc8.jar"
    },
    DATASOURCE_MAP["DB"]["MSSQL"]: {
        "jdbc_url": "jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jar_path": "./jdbc-drivers/mssql-jdbc-12.8.1.jre8.jar"
    }
}


def get_spark_session(app_name="ETLJob", config_options=None, jars=None, packages=None):
    """
    Initialize and return a Spark session with optional configuration settings.

    Args:
    - app_name (str): Name of the Spark application.
    - config_options (dict): Additional key-value pairs for Spark configurations.
    - jars (str): Comma-separated list of JARs to include in the classpath.
    - packages (str): Comma-separated list of Maven coordinates for external packages.

    Returns:
    - SparkSession: Initialized Spark session with provided configurations.
    """
    builder = SparkSession.builder.appName(app_name)

    if jars:
        builder = builder.config("spark.jars", jars)

    if packages:
        builder = builder.config("spark.jars.packages", packages)

    if config_options:
        for key, value in config_options.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    return spark


# JDBC connections
def lambda_handler(connection_id="bharwer_1727339262065-1729659952682"):
    # cw_logger = CloudWatchLogger(LOG_GROUP)
    etl_logger = ETLLogger(log_file="etl_job.log")
    local_logs = etl_logger.get_logger()

    
    connection_params = fetch_connection_params(
        table_name=CONNECTOR_DYNAMO_TABLE,
        connection_id=connection_id
    )
    
    if not connection_params:
        # cw_logger.log(f"Connection parameters not found ind db for id: {connection_id}")
        local_logs.error(f"Connection parameters not found ind db for id: {connection_id}")
    
    source_type = connection_params["source"]

    valid_source_types = set(DATASOURCE_MAP["DB"].values()).union(DATASOURCE_MAP["FILE"].values())

    if source_type not in valid_source_types:
        # cw_logger.log(f"Unsupported source type: {source_type}")
        local_logs.error(f"Unsupported source type: {source_type}")
        return

    if source_type == DATASOURCE_MAP["FILE"]["JSON"]: 
        data_source = JSONDataSource(connection_params["file_path"]) # s3 file path
        spark = get_spark_session()

    elif source_type == DATASOURCE_MAP["FILE"]["CSV"]:
        data_source = CSVDataSource(connection_params["file_path"]) # s3 file path
        spark = get_spark_session()

    elif source_type == DATASOURCE_MAP["FILE"]["XML"]:
        data_source = XMLDataSource(
            connection_params["file_path"], connection_params["row_tag"] # s3 file path with row tag meta data
        )
        spark = get_spark_session(packages="com.databricks:spark-xml_2.12:0.14.0")

    elif source_type in DATASOURCE_MAP["DB"].values():

        db_creds = get_db_creds(connection_params, source_type)

        if not db_creds:
            # cw_logger.log(f"No data found for id: {connection_id}")
            local_logs.error(f"No data found for id: {connection_id}")
            return

        if not db_creds:
            # cw_logger.log(f"No db creds found for id: {connection_id}, source: {source_type}")
            local_logs.error(
                f"No db creds found for id: {connection_id}, source: {source_type}"
            )
            return

        db_config = DATABASE_CONFIG.get(source_type)
        jdbc_url = db_config["jdbc_url"].format(
            host=db_creds["host"],
            port=db_creds["port"],
            database=db_creds["database"]
        )
        driver = db_config["driver"]
        jar_path = db_config["jar_path"]

        spark = get_spark_session(jars=jar_path)

        data_source = JDBCDataSource(
            jdbc_url=jdbc_url,
            user_name=db_creds["username"],
            password=db_creds["password"],
            driver=driver
        )

        if data_source.check_connection(spark):
            # cw_logger.log(f"No db creds found for conn id: {connection_id}, source: {source_type} database successful.")
            local_logs.info(f"Connection to conn id: {connection_id}, {source_type} database successful.")
            tables_df = data_source.show_tables(spark, db_type=source_type)

            if tables_df:
                table_names = tables_df.select("table_name").rdd.flatMap(lambda x: x).collect()
                local_logs.info(f"Tables fetched from the database: {table_names}")
            else:
                local_logs.warning("No tables found in the database.")
        else:
            local_logs.error(f"Failed to connect to the {source_type} database.")
        
        # model_mapping = fetch_model_mapping(
        #     table_name=MODEL_MAPPING_DYNAMO_TABLE, 
        #     connection_id="avtar_1726485754460-1728890772659"
        # )

        # print(model_mapping)
        

    spark.stop()

if __name__ == "__main__":
    lambda_handler()


# def main():

#     mysql_jdbc_path = "./jdbc-drivers/mysql-connector-j-9.1.0.jar"
#     postgres_jdbc_path = "./jdbc-drivers/postgresql-42.7.4.jar"
#     mssql_jdbc_path = "./jdbc-drivers/mssql-jdbc-12.8.1.jre8.jar"
#     oracle_jdbc_path = "./jdbc-drivers/oracle-jdbc8.jar"


#     spark = SparkSession.builder.appName("ETLJob").config("spark.jars", mssql_jdbc_path).getOrCreate()

#     # mysql
#     mysql_url = "jdbc:mysql://localhost:3306/test_db"
#     mysql_driver = "com.mysql.cj.jdbc.Driver"
#     user = "test_user"
#     password = "test_password"
#     table_name = "users"

#     ## postgres
#     postgres_url = "jdbc:postgresql://localhost:5432/test_db"
#     postgres_driver = "org.postgresql.Driver"
#     user = "test_user"
#     password = "test_password"
#     table_name = "users"

#     ## ms-sql
#     mssql_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
#     user = "sa"
#     password = "Passw0rd"
#     table_name = "users"
#     mssql_url = f"jdbc:sqlserver://localhost:1433;databaseName=demo;encrypt=true;trustServerCertificate=true"

#     # oracle
#     # oracle_url = "jdbc:oracle:thin:@localhost:1521/FREEPDB1"
#     # oracle_driver = "oracle.jdbc.driver.OracleDriver"
#     # user = "test_user"
#     # password = "test_password"
#     # table_name = "users"

#     df = spark.read \
#                 .format("jdbc") \
#                 .option("url", mssql_url) \
#                 .option("dbtable", table_name) \
#                 .option("user", user) \
#                 .option("password", password) \
#                 .option("driver", mssql_driver) \
#                 .load()
#     # data_source = JDBCDataSource(
#     #     jdbc_url=oracle_url,
#     #     table_name=table_name,
#     #     user_name=user,
#     #     password=password,
#     #     driver=oracle_driver
#     # )

#     # df = data_source.fetch_data(spark)
#     # df.printSchema()
#     df.show()


#     spark.stop()

# if __name__ == "__main__":
#     main()

# db_creds = {
#             "host": "onefitness-dev.cucwth4ve3e9.ap-southeast-1.rds.amazonaws.com",
#             "port": 3306,
#             "database": "onefitness_dev",
#             "username": "developer",
#             "password": "IKPo4iLMv0eJddEm",
#         }
# def main():

#     server = db_creds["host"]
#     user = db_creds["username"]
#     password = db_creds["password"]
#     table_name = ""
#     database=db_creds["database"]

#     spark = SparkSession.builder \
#     .appName("ODBC Data Fetch Example") \
#     .getOrCreate()

#     mysql_config = MySQLConfig(
#         server=server,
#         user=user,
#         password=password,
#         database=database
#     )

#     postgres_config = PostgreSQLConfig(
#         server=server,
#         user=user,
#         password=password,
#         database=database
#     )

#     connector = DatabaseConnector(config=mysql_config)

#     query = f"SHOW tables;"

#     odbc_data_source = ODBCDataSource(query=query, connector=connector)

#     df = odbc_data_source.fetch_data(spark)

#     df.show()


# if __name__ == "__main__":
#     main()
