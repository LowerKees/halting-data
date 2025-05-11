"""Define expectations for the transactions data set."""

from great_expectations import expectations as gxe

exps = [
    gxe.ExpectColumnToExist(column="id"),
    gxe.ExpectColumnToExist(column="amt"),
    gxe.ExpectColumnToExist(column="from"),
    gxe.ExpectColumnToExist(column="to"),
    gxe.ExpectColumnToExist(column="dts"),
    gxe.ExpectColumnValuesToBeInTypeList(column="id", type_list=["StringType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="amt", type_list=["DecimalType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="from", type_list=["StringType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="to", type_list=["StringType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="dts", type_list=["TimestampType"]),
]
