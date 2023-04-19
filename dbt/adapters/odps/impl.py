import agate
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.adapters.base.meta import available
from dbt.contracts.graph.manifest import Manifest
from typing import List, Set, cast
from typing_extensions import TypeAlias
from odps import ODPS
from odps.models import Table, TableSchema
from .relation import OdpsRelation
from .colums import OdpsColumn
from .connections import ODPSConnectionManager, ODPSCredentials, SchemaTypes

LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
SHOW_CREATE_TABLE_MACRO_NAME = "show_create_table"
RENAME_RELATION_MACRO_NAME = "rename_relation"


class ODPSAdapter(SQLAdapter):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = ODPSConnectionManager
    Relation: TypeAlias = OdpsRelation
    Column: TypeAlias = OdpsColumn

    @property
    def odps(self) -> ODPS:
        return self.connections.get_thread_connection().handle._odps

    @property
    def credentials(self) -> ODPSCredentials:
        return self.config.credentials

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # TODO CT-211
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "double" if decimals else "bigint"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "datetime"

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
        if self.credentials.schema_type == SchemaTypes.PREFIX_SCHEMA:
            relations = self.list_relations_without_caching(relation)
            for relation in relations:
                if relation.schema == relation.schema:
                    self.drop_relation(relation)

    def quote(self, identifier):
        return "`{}`".format(identifier)

    @available
    def quote_for_rename(self, relation: OdpsRelation) -> str:
        if relation.schema_type == SchemaTypes.PREFIX_SCHEMA:
            return relation.quote_schema(relation.schema, relation.identifier)
        return self.quote(relation.identifier)

    def check_schema_exists(self, database: str, schema: str) -> bool:
        """always return true, as ODPS does not have schemas."""
        return True

    def list_schemas(self, database: str) -> List[str]:
        if self.credentials.schema_type == SchemaTypes.PREFIX_SCHEMA:
            relations = self.list_relations_without_caching()
            return list(dict.fromkeys([r.schema for r in relations]))
        return []

    def list_relations_without_caching(
        self,
        schema_relation: OdpsRelation = None,
    ) -> List[OdpsRelation]:
        # TODO: unit test
        prefix = None
        if schema_relation is not None:
            # when no identifier, and schema exist, using schema as prefix to query
            if (
                schema_relation.identifier is None
                and schema_relation.schema is not None
                and schema_relation.schema_type == SchemaTypes.PREFIX_SCHEMA
            ):
                prefix = OdpsRelation.quote_schema(schema_relation.schema)

        relations = []
        for table in self.odps.list_tables(prefix=prefix):
            relations.append(OdpsRelation.from_odps_table(table))

        return relations

    def get_odps_table_by_relation(self, relation: OdpsRelation):
        if relation.schema_type == SchemaTypes.PREFIX_SCHEMA:
            relation_name = (
                relation.include(identifier=True, database=False, schema=False)
                .quote(identifier=False)
                .render()
            )
            return self.odps.get_table(relation_name, project=relation.database)
        else:
            return self.odps.get_table(relation.identifier, project=relation, schema=relation.schema)

    def get_columns_in_relation(self, relation: OdpsRelation):
        odps_table = self.get_odps_table_by_relation(relation)
        return [OdpsColumn.from_odps_column(column) for column in odps_table.schema.get_columns()]

    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:
        rows = []
        for schema in schemas:
            prefix = None
            odps_schema = schema
            if self.credentials.schema_type == SchemaTypes.PREFIX_SCHEMA:
                prefix = OdpsRelation.quote_schema(schema)
                odps_schema = None

            for table in self.odps.list_tables(
                prefix=prefix, project=information_schema.database, schema=odps_schema
            ):
                table = cast(Table, table)
                table_name = str(table.name).replace(prefix, "") if prefix else str(table.name)
                table_rows = (
                    information_schema.database,
                    schema,
                    table_name,
                    "VIEW" if table.is_virtual_view else "TABLE",
                    table.comment,
                    table.owner,
                )
                for i, column in enumerate(table.schema.get_columns()):
                    column = cast(TableSchema.TableColumn, column)
                    column_rows = table_rows + (column.name, i, column.type, column.comment)
                    rows.append(column_rows)
        table = agate.Table(
            rows,
            [
                "table_database",
                "table_schema",
                "table_name",
                "table_type",
                "table_comment",
                "table_owner",
                "column_name",
                "column_index",
                "column_type",
                "column_comment",
            ],
        )
        return table


# may require more build out to make more user friendly to confer with team and community.
