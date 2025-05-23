import pathlib

from pyspark.sql import SparkSession


class Processor:
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark

    def process_data(self, input_path: pathlib.Path, output_path: pathlib.Path):
        """Dummy data processor.

        Lets pretend a lot of important transformations take place
        here.

        Args:
            input_path: a pathlib.Path instance containing a path to the input data
                file.
            output_path: a pathlib.Path instance containing a directory path to load to
                data to.
        """
        if input_path.parts[-1] == "invalid.csv":
            print("Using invalid schema...")
            schema = "id STRING, amt DECIMAL(18,8), from STRING, to STRING, dts TIMESTAMP, curr STRING"
        else:
            print("Using valid schema...")
            schema = (
                "id STRING, amt DECIMAL(18,8), from STRING, to STRING, dts TIMESTAMP"
            )
        df = self.spark.read.schema(schema).csv(
            path=str(input_path), header=True, sep=","
        )
        ...
        df.coalesce(1)
        df.write.format("delta").option("mergeSchema", True).mode("append").save(
            path=str(output_path)
        )
