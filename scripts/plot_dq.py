import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
from data import DQ
from pyspark.sql.functions import col

from shared.middleware import spark

matplotlib.use("Qt5Agg")

df = spark.read.format("delta").load(str(DQ))
df = df.where(
    "result_array.expectation_config.type = 'expect_column_values_to_be_unique'"
)
df = df.withColumn("result", col("result_array.success"))

df = df.toPandas()

plt.figure(figsize=(10, 6))
sns.lineplot(data=df, x="__load_dts", y="result", color="red", linestyle="--")
plt.title("Uniqueness check of the transaction id")
plt.xlabel("Time")
plt.ylabel("Is unique")
plt.grid(True)
plt.show()
