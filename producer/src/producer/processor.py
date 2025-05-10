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
        df = self.spark.read.csv(path=str(input_path), header=True, sep=",")
        ...
        df.write.format("delta").mode("append").save(path=str(output_path))
