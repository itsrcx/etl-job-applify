from pyspark.sql import DataFrame

DATASTORE_MAP = {
    "REDSHIFT": "redshift"
}

class DataStore:
    def upsert_data(self, data):
        raise NotImplementedError()

class PostgresDataStore(DataStore):
    def __init__(self, jdbc_url, db_table, postgres_user, postgres_password):
        self.jdbc_url = jdbc_url
        self.db_table = db_table
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password

    def upsert_data(self, data: DataFrame):
        # Write the cleaned data to a staging table in PostgreSQL
        staging_table = self.db_table + "_staging"
        
        # Stage 1: Write to the staging table
        data.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", staging_table) \
            .option("user", self.postgres_user) \
            .option("password", self.postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        # Stage 2: Perform an upsert in PostgreSQL using SQL queries
        upsert_sql = f"""
        BEGIN;
        DELETE FROM {self.db_table} USING {staging_table}
        WHERE {self.db_table}.id = {staging_table}.id;

        INSERT INTO {self.db_table}
        SELECT * FROM {staging_table};

        DROP TABLE {staging_table};
        COMMIT;
        """
        print(f"Executing upsert for PostgreSQL:\n{upsert_sql}")

class RedshiftDataStore(DataStore):
    def __init__(self, spark, db_url, db_user, db_password, db_driver):
        self.spark = spark
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.db_driver = db_driver

    #Write data to redshift
    def upsert_data(self, df: DataFrame, table_name: str):
        df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", table_name) \
            .option("user", self.db_user) \
            .option("password", self.db_password) \
            .option("driver", self.db_driver) \
            .mode("append") \
            .save()
        print("data loaded to the redshift")

class DataLakeDataStore(DataStore):
    def upsert_data(self, data):
        # Code to upsert data into Data Lake
        pass
