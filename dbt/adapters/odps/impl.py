import agate
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.odps import ODPSConnectionManager


class ODPSAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ODPSConnectionManager

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_boolean_type(cls, agate_table, col_idx: int) -> str:
        return "boolean"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx: int) -> str:
        raise NotImplementedError(
            "ODPS does not support a native number type. Use a float or integer instead."
        )

    @classmethod
    def convert_text_type(cls, agate_table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise NotImplementedError("ODPS does not support a native time type. Use a timestamp instead.")

    @classmethod
    def create_schema(cls, profile, schema, model_name=None):
        raise NotImplementedError("ODPS does not support schema creation")

    @classmethod
    def drop_schema(cls, profile, schema, model_name=None):
        raise NotImplementedError("ODPS does not support schema deletion")


# may require more build out to make more user friendly to confer with team and community.
