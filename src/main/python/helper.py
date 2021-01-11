from pyspark.sql.types import *


def read_schema(schema_string):
    struct_fields = schema_string.split("|")
    struct_type = StructType()
    for field in struct_fields:
        field_name, field_type = field.split(",")
        struct_type.add(field_name, eval(field_type), True)
    return struct_type
