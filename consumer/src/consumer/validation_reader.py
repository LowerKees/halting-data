from shared.src.quality_gates.middleware import data_store, spark

df = spark.read.format("delta").load(str(data_store / "dq" / "transactions"))
df.show()
