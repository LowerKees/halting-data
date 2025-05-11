# General

This repo contains three nodes in a data mesh:
1. A 'producer' node that produces transactional data
2. A 'data store' node that acts the data product's output ports
3. A 'consumer' node that retrieves data from the data store's 'gold' layer

The set of data that is produced by the produces has the following schema:

```python
StructType(
    [
        StructField("id", StringType(), False),
        StructField("amt", DecimalType(16, 8), False),
        StructField("from", StringType(), False),
        StructField("to", StringType(), False),
        StructField("dts", TimestampType(), False),
    ]
)
```
Below is a sample of five random records from the data set.
![Five records from the transaction set.](/static/sample_transactions.png)

Running the producer in a valid scenario:
```bash
uv run producer/src/producer/main.py data_store/transactions/src/valid.csv data_store/transactions/gold
```