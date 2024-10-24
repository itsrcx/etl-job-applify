from pyspark.sql import DataFrame

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
    def upsert_data(self, data):
        # Code to upsert data into Redshift
        pass

class DataLakeDataStore(DataStore):
    def upsert_data(self, data):
        # Code to upsert data into Data Lake
        pass
