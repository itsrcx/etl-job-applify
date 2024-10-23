import pyodbc
from services.odbc_db_config import DBConfig

from pyspark.sql import SparkSession, Row
from pyspark.sql.utils import AnalysisException
from pyspark.errors import AnalysisException


DATASOURCE_MAP = {
    "DB": {
        "MYSQL": "mysql",
        "POSTGRES": "postgres",
        "ORACLE": "oracle",
        "MSSQL": "mssql",
    },
    "FILE": {
        "JSON": "json", 
        "CSV": "csv", 
        "XML": "xml"
    },
}

class DataSource:
    def fetch_data(self):
        raise NotImplementedError()

class JSONDataSource(DataSource):
    def __init__(self, file_path):
        self.file_path = file_path

    def fetch_data(self, spark: SparkSession):
        try:
            df = spark.read\
                .option("multiline","true")\
                .option("mode", "PERMISSIVE")\
                .json(self.file_path)
            return df
        except AnalysisException as e:
            print(f"Error loading Json: {e}")

class CSVDataSource(DataSource):
    def __init__(self, file_path):
        self.file_path = file_path

    def fetch_data(self, spark: SparkSession):
        try:
            df = spark.read\
                .option("mode", "PERMISSIVE")\
                .csv(self.file_path, header=True, inferSchema=True)
            return df
        except AnalysisException as e:
            print(f"Error loading CSV: {e}")

class XMLDataSource(DataSource):
    def __init__(self, file_path, row_tag):
        self.file_path = file_path
        self.row_tag = row_tag

    def fetch_data(self, spark: SparkSession):
        try:
            df = spark.read\
                .format("com.databricks.spark.xml")\
                .option("rowTag", self.row_tag)\
                .load(self.file_path)
            return df
        
        except AnalysisException as e:
            print(f"Error loading XML: {e}")

class JDBCDataSource(DataSource):
    """Establishes a JDBC connection to multiple data sources"""
    def __init__(self, jdbc_url, user_name, password, driver):
        self.jdbc_url = jdbc_url
        self.user_name = user_name
        self.password = password
        self.driver = driver

    def fetch_data(self, spark: SparkSession, table_name):
        """Fetches all data for the provided table using spark."""
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.user_name) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .load()
            return df
        except Exception as e:
            print(f"Error fecthing data: {e}")
    
    def check_connection(self, spark: SparkSession):
        """Checks the connection to the database by executing a simple query."""
        try:
            # Attempting to run a simple query to check the connection
            spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", "SELECT 1 AS test_column") \
                .option("user", self.user_name) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .load()
            return True
        except AnalysisException as e:
            print(f"AnalysisException: {e}")
            return False
        except Exception as e:
            print(f"Failed to connect to the database: {e}")
            return False

    def show_tables(self, spark: SparkSession, db_type: str):
        """Fetches the list of tables from the database."""
        if db_type == DATASOURCE_MAP["DB"]["MYSQL"]:
            table_query = f"(SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.jdbc_url.split('/')[-1]}') AS tables"
        elif db_type == DATASOURCE_MAP["DB"]["POSTGRES"]:
            table_query = "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') AS tables"
        elif db_type == DATASOURCE_MAP["DB"]["ORACLE"]:
            table_query = "(SELECT table_name FROM user_tables) tables"
        elif db_type == DATASOURCE_MAP["DB"]["MSSQL"]:
            table_query =  "(SELECT table_name FROM INFORMATION_SCHEMA.TABLES) AS tables"
        try: 
            tables_df = spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", table_query) \
                .option("user", self.user_name) \
                .option("password", self.password) \
                .option("driver", self.driver) \
                .load()
            return tables_df
        except Exception as e:
            print(f"Error while showing tables: {e}")

class DatabaseConnector:
    """Handles database connection using dependency injection."""
    def __init__(self, config: DBConfig):
        self.config = config

    def connect(self):
        """Establishes an ODBC connection using pyodbc."""
        try:
            conn_str = self.config.get_connection_string()
            conn = pyodbc.connect(conn_str)
            print("ODBC Connection successful")
            return conn
        except pyodbc.Error as e:
            print("Error in connection: ", e)
            return None
    
    def fetch_data(self, query: str):
        """Fetches data using ODBC and returns the raw result."""
        conn = self.connect()
        if conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            conn.close()
            return columns, rows
        return None, None

class ODBCDataSource(DataSource):
    """DataSource for loading data from a database via ODBC without pandas."""
    def __init__(self, query: str, connector: DatabaseConnector):
        self.query = query
        self.connector = connector

    def fetch_data(self, spark: SparkSession):
        """Fetches data from ODBC and converts it directly to a Spark DataFrame."""
        columns, rows = self.connector.fetch_data(self.query)
        
        if columns and rows:
            # Convert rows to a list of Spark Rows
            spark_rows = [Row(**dict(zip(columns, row))) for row in rows]
            
            # Create a Spark DataFrame from the Spark Rows
            spark_df = spark.createDataFrame(spark_rows)
            return spark_df
        else:
            print("No data fetched")
            return None

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