import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType, TimestampType, DateType

class DataTransformer:
    def clean_data(self, df):
        raise NotImplementedError()

class SparkDataTransformer(DataTransformer):
    def __init__(self, spark):
        self.spark = spark

    def clean_data(self, df):

        df_cleaned = df.dropDuplicates()

        return df_cleaned
    
    def load_schema(self, schema_file):

        with open(schema_file, 'r') as file:
            schema = yaml.safe_load(file)
        return schema

    def map_columns_to_target(self, df, schema):
        for column in schema['target_schema']['columns']:
            source_column = column['source']
            target_column = column['name']
            target_type = column['type']

            if target_type == "string":
                df = df.withColumn(target_column, F.col(source_column).cast(StringType()))
            elif target_type == "integer":
                df = df.withColumn(target_column, F.col(source_column).cast(IntegerType()))
            elif target_type == "float":
                df = df.withColumn(target_column, F.col(source_column).cast(FloatType()))
            elif target_type == "boolean":
                df = df.withColumn(target_column, F.col(source_column).cast(BooleanType()))
            elif target_type == "date":
                date_format = column.get('format', 'yyyy-MM-dd')
                df = df.withColumn(target_column, F.to_date(F.col(source_column), date_format))
            elif target_type == "timestamp":
                timestamp_format = column.get('format', 'yyyy-MM-dd HH:mm:ss')
                df = df.withColumn(target_column, F.to_timestamp(F.col(source_column), timestamp_format))
            else:
                raise ValueError(f"Unsupported data type: {target_type}")

        return df