from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

result_schema = StructType(
    [
        StructField("success", BooleanType(), True),
        StructField(
            "results",
            ArrayType(
                StructType(
                    [
                        StructField("success", BooleanType(), True),
                        StructField(
                            "expectation_config",
                            StructType(
                                [
                                    StructField("type", StringType(), True),
                                    StructField(
                                        "kwargs",
                                        MapType(
                                            keyType=StringType(), valueType=StringType()
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "meta",
                                        MapType(StringType(), StringType(), True),
                                        True,
                                    ),
                                    StructField("id", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "result", MapType(StringType(), StringType(), True), True
                        ),
                        StructField(
                            "meta", MapType(StringType(), StringType(), True), True
                        ),
                        StructField(
                            "exception_info",
                            StructType(
                                [
                                    StructField(
                                        "raised_exception", BooleanType(), True
                                    ),
                                    StructField(
                                        "exception_traceback", StringType(), True
                                    ),
                                    StructField(
                                        "exception_message", StringType(), True
                                    ),
                                ]
                            ),
                            True,
                        ),
                    ]
                )
            ),
            True,
        ),
        StructField("suite_name", StringType(), True),
        StructField(
            "suite_parameters", MapType(StringType(), StringType(), True), True
        ),
        StructField(
            "statistics",
            StructType(
                [
                    StructField("evaluated_expectations", IntegerType(), True),
                    StructField("successful_expectations", IntegerType(), True),
                    StructField("unsuccessful_expectations", IntegerType(), True),
                    StructField("success_percent", FloatType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "meta",
            StructType(
                [
                    StructField("great_expectations_version", StringType(), True),
                    StructField(
                        "batch_spec",
                        StructType([StructField("batch_data", StringType(), True)]),
                        True,
                    ),
                    StructField(
                        "batch_markers",
                        StructType([StructField("ge_load_time", StringType(), True)]),
                        True,
                    ),
                    StructField(
                        "active_batch_definition",
                        StructType(
                            [
                                StructField("datasource_name", StringType(), True),
                                StructField("data_connector_name", StringType(), True),
                                StructField("data_asset_name", StringType(), True),
                                StructField(
                                    "batch_identifiers",
                                    StructType(
                                        [StructField("dataframe", StringType(), True)]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                    StructField("validation_id", StringType(), True),
                    StructField("checkpoint_id", StringType(), True),
                    StructField(
                        "run_id",
                        StructType(
                            [
                                StructField("run_name", StringType(), True),
                                StructField("run_time", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("validation_time", StringType(), True),
                    StructField(
                        "batch_parameters",
                        StructType([StructField("dataframe", StringType(), True)]),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("id", StringType(), True),
    ]
)
