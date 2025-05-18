"""Define expectations for the transactions data set."""

from great_expectations import expectations as gxe

col_exp = [
    gxe.ExpectColumnToExist(column="id"),
    gxe.ExpectColumnToExist(column="amt"),
    gxe.ExpectColumnToExist(column="from"),
    gxe.ExpectColumnToExist(column="to"),
    gxe.ExpectColumnToExist(column="dts"),
]

type_exp = [
    gxe.ExpectColumnValuesToBeInTypeList(column="id", type_list=["StringType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="amt", type_list=["DecimalType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="from", type_list=["StringType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="to", type_list=["StringType"]),
    gxe.ExpectColumnValuesToBeInTypeList(column="dts", type_list=["TimestampType"]),
]

unique_exp = [gxe.ExpectColumnValuesToBeUnique(column="id")]
