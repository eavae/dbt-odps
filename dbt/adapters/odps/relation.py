import re
from dataclasses import dataclass, field
from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import Policy, RelationType, ComponentName
from odps.models.table import Table
from typing import Iterator, Optional, Tuple
from .connections import SchemaTypes


@dataclass
class OdpsIncludePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class OdpsRelation(BaseRelation):
    include_policy: Policy = field(default_factory=lambda: OdpsIncludePolicy())
    quote_character: str = "`"
    schema_type: SchemaTypes = SchemaTypes.PREFIX_SCHEMA

    def _render_iterator(self) -> Iterator[Tuple[Optional[ComponentName], Optional[str]]]:
        for key in ComponentName:
            path_part: Optional[str] = None
            if self.include_policy.get_part(key):
                path_part = self.path.get_part(key)
                if (
                    key == ComponentName.Identifier
                    and self.schema_type == SchemaTypes.PREFIX_SCHEMA
                    and self.path.schema
                    and self.path.schema != "public"
                ):
                    path_part = self.quote_schema(self.path.schema, path_part)
                if path_part is not None and self.quote_policy.get_part(key):
                    path_part = self.quoted(path_part)
            yield key, path_part

    @classmethod
    def quote_schema(cls, schema: str, identifier: str = "") -> str:
        return f"__{schema}__{identifier}"

    @classmethod
    def parse_schema(cls, identifier: str) -> Tuple[str, str]:
        matches = re.match("^__([\w\-_]+)__(.*)$", identifier)
        if matches:
            schema = matches.group(1)
            identifier = matches.group(2)
        else:
            schema = "public"
        return schema, identifier

    @classmethod
    def from_odps_table(cls, table: Table, schema_type=SchemaTypes.PREFIX_SCHEMA):
        identifier = table.name
        schema = table.get_schema()
        schema = schema.name if schema else "public"
        if schema_type == SchemaTypes.PREFIX_SCHEMA:
            schema, identifier = cls.parse_schema(identifier)

        return cls.create(
            database=table.project.name,
            schema=schema,
            identifier=identifier,
            type=RelationType.View if table.is_virtual_view else RelationType.Table,
            schema_type=schema_type,
        )
