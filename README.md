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

Checking data expectations and publishing them to the data store.
```bash
uv run producer/src/producer/scanner/scanner.py
```

Consumper validation the data
```bash
uv run consumer/src/consumer/validator.py <run_id> | <iso_date>
```
The deeply nested Great Expectations output
```json
{
  "success": true,
  "results": [
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_to_exist",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "id"
        },
        "meta": {},
        "id": "12247b4a-3ec6-408f-b9ad-a310dd23d0c5"
      },
      "result": {},
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "id",
          "type_list": ["StringType"]
        },
        "meta": {},
        "id": "fe70966c-795b-42bb-b9ed-eed27514045f"
      },
      "result": { "observed_value": "StringType" },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_be_unique",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "id"
        },
        "meta": {},
        "id": "b7ba57e3-791e-4875-8e3c-41836c24221f"
      },
      "result": {
        "element_count": 100000,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": []
      },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_to_exist",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "amt"
        },
        "meta": {},
        "id": "409a2a49-6d52-4b7a-b0bc-fc1de67b5be1"
      },
      "result": {},
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "amt",
          "type_list": ["DecimalType"]
        },
        "meta": {},
        "id": "f9c71a0f-9595-4185-b582-4b4b2b75de7c"
      },
      "result": { "observed_value": "DecimalType" },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_to_exist",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "from"
        },
        "meta": {},
        "id": "b24d23c4-77d3-4045-a43a-a4499b245f25"
      },
      "result": {},
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "from",
          "type_list": ["StringType"]
        },
        "meta": {},
        "id": "649e9c99-2d0f-4bc4-a27b-eee98c8d54ba"
      },
      "result": { "observed_value": "StringType" },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_to_exist",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "to"
        },
        "meta": {},
        "id": "1412fdd5-796f-426a-80dc-64e88a8c1aa6"
      },
      "result": {},
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "to",
          "type_list": ["StringType"]
        },
        "meta": {},
        "id": "714c59e7-5c03-4726-9fdc-2a561ae5c0f1"
      },
      "result": { "observed_value": "StringType" },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_to_exist",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "dts"
        },
        "meta": {},
        "id": "ded5500f-a68c-424f-b34c-48cc8fa8e047"
      },
      "result": {},
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    },
    {
      "success": true,
      "expectation_config": {
        "type": "expect_column_values_to_be_in_type_list",
        "kwargs": {
          "batch_id": "transactions-transaction_data",
          "column": "dts",
          "type_list": ["TimestampType"]
        },
        "meta": {},
        "id": "7504961a-9fbd-4075-9d11-a868fb2e8346"
      },
      "result": { "observed_value": "TimestampType" },
      "meta": {},
      "exception_info": {
        "raised_exception": false,
        "exception_traceback": null,
        "exception_message": null
      }
    }
  ],
  "suite_name": "transaction",
  "suite_parameters": {},
  "statistics": {
    "evaluated_expectations": 11,
    "successful_expectations": 11,
    "unsuccessful_expectations": 0,
    "success_percent": 100.0
  },
  "meta": {
    "great_expectations_version": "1.4.1",
    "batch_spec": { "batch_data": "SparkDataFrame" },
    "batch_markers": { "ge_load_time": "20250513T181145.205656Z" },
    "active_batch_definition": {
      "datasource_name": "transactions",
      "data_connector_name": "fluent",
      "data_asset_name": "transaction_data",
      "batch_identifiers": { "dataframe": "<DATAFRAME>" }
    },
    "validation_id": "d26e0204-83ef-4bfa-ac39-6c1ec14fcc43",
    "checkpoint_id": null,
    "run_id": {
      "run_name": null,
      "run_time": "2025-05-13T20:11:51.913729+02:00"
    },
    "validation_time": "2025-05-13T18:11:51.913729+00:00",
    "batch_parameters": { "dataframe": "<DATAFRAME>" }
  },
  "id": null
}
```