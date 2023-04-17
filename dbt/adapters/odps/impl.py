import agate
from dataclasses import dataclass, field
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.odps import ODPSConnectionManager
from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import Policy, RelationType
from typing import List
from typing_extensions import TypeAlias
from .errors import NotTableError

LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
SHOW_CREATE_TABLE_MACRO_NAME = "show_create_table"


@dataclass
class OdpsIncludePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class OdpsRelation(BaseRelation):
    include_policy: Policy = field(default_factory=lambda: OdpsIncludePolicy())
    quote_character: str = "`"


class ODPSAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ODPSConnectionManager
    Relation: TypeAlias = OdpsRelation

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # TODO CT-211
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "double" if decimals else "bigint"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        raise NotImplementedError("ODPS does not support a native time type. Use a timestamp instead.")

    @classmethod
    def convert_text_type(cls, agate_table, col_idx: int) -> str:
        return "string"

    def create_schema(self, relation: BaseRelation) -> None:
        """ODPS does not support schemas, so this is a no-op"""
        pass

    def drop_schema(self, relation: BaseRelation) -> None:
        """ODPS does not support schemas, so this is a no-op"""
        pass

    def quote(self, identifier):
        return "`{}`".format(identifier)

    def check_schema_exists(self, database: str, schema: str) -> bool:
        """always return true, as ODPS does not have schemas."""
        return True

    def list_schemas(self, database: str) -> List[str]:
        """always return empty list, as ODPS does not have schemas."""
        return []

    def list_relations_without_caching(
        self,
        schema_relation: BaseRelation,
    ) -> List[OdpsRelation]:
        kwargs = {"schema_relation": schema_relation}
        table_names = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        pure_table_names = [tbl.split(":")[-1] for tbl in table_names.split("\n") if tbl != ""]

        relations = []
        for table_name in pure_table_names:
            try:
                kwargs = {
                    "relation": OdpsRelation.create(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=table_name,
                    )
                }
                self.execute_macro(SHOW_CREATE_TABLE_MACRO_NAME, kwargs=kwargs)
                table_type = RelationType.Table
            except NotTableError:
                table_type = RelationType.View

            relations.append(
                OdpsRelation.create(
                    database=schema_relation.database,
                    schema=schema_relation.schema,
                    identifier=table_name,
                    type=table_type,
                )
            )
        return relations


# may require more build out to make more user friendly to confer with team and community.
