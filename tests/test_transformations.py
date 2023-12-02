import unittest
import utils.transformations as tr
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructType,
    StructField,
    IntegerType,
)


class TestTransformations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession \
            .builder \
            .appName("Unit Test") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_schema(self, df1: DataFrame, df2: DataFrame, check_nullable=True):
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, df1.schema.fields)]
        fields2 = [*map(field_list, df2.schema.fields)]
        if check_nullable:
            res = set(fields1) == set(fields2)
        else:
            res = set([field[:-1] for field in fields1]) == set([field[:-1] for field in fields2])
        return res

    def test_data(self, df1: DataFrame, df2: DataFrame):
        data1 = df1.collect()
        data2 = df2.collect()
        return set(data1) == set(data2)


class TestApodTransformations(TestTransformations):

    def test_apod(self):
        input_data = [
            [
                "2023-06-22",
                "Lorem Ipsum is important!",
                "https://apod.nasa.gov/apod/image/2306/lorem.jpg",
                "image",
                "v1",
                "Galactic Cirrus: Lorem Ipsum",
                "https://apod.nasa.gov/apod/image/2306/lorem1024.jpg",
                "https://apod.nasa.gov/apod/image/2306/lorem1024.jpg",
                "https://apod.nasa.gov/apod/image/2306/lorem.jpg",
                2023,
                7,
                25,
            ]
        ]
        input_schema = StructType(
            [
                StructField("date", StringType(), True),
                StructField("explanation", StringType(), True),
                StructField("hdurl", StringType(), True),
                StructField("media_type", StringType(), True),
                StructField("service_version", StringType(), True),
                StructField("title", StringType(), True),
                StructField("url", StringType(), True),
                StructField("file_name", StringType(), True),
                StructField("hd_file_name", StringType(), True),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("day", IntegerType(), False),
            ]
        )
        input_df = self.spark.createDataFrame(input_data, input_schema)
        actual_df = tr.apod(input_df)

        expected_data = [
            [
                "2023-06-22",
                "Lorem Ipsum is important!",
                "https://apod.nasa.gov/apod/image/2306/lorem.jpg",
                "image",
                "v1",
                "GALACTIC CIRRUS: LOREM IPSUM",
                "https://apod.nasa.gov/apod/image/2306/lorem1024.jpg",
                "lorem1024.jpg",
                "lorem.jpg",
                2023,
                7,
                25,
            ]
        ]
        expected_schema = StructType(
            [
                StructField("date", DateType(), True),
                StructField("explanation", StringType(), True),
                StructField("hdurl", StringType(), True),
                StructField("media_type", StringType(), True),
                StructField("service_version", StringType(), True),
                StructField("title", StringType(), True),
                StructField("url", StringType(), True),
                StructField("file_name", StringType(), True),
                StructField("hd_file_name", StringType(), True),
                StructField("year", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("day", IntegerType(), False),
            ]
        )
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        self.assertTrue(self.test_schema(actual_df, expected_df))