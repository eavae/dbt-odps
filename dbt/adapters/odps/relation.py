from dataclasses import dataclass, field
from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import Policy, RelationType
from odps.models.table import Table


@dataclass
class OdpsIncludePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class OdpsRelation(BaseRelation):
    include_policy: Policy = field(default_factory=lambda: OdpsIncludePolicy())
    quote_character: str = "`"

    @classmethod
    def create(cls, database=None, schema=None, identifier=None, type=None, **kwargs):
        if schema != "default":
            kwargs.update(
                {
                    "include_policy": OdpsIncludePolicy(schema=True),
                }
            )
        return super().create(database, schema, identifier, type, **kwargs)

    @classmethod
    def from_odps_table(cls, table: Table):
        identifier = table.name
        schema = table.get_schema()
        schema = schema.name if schema else "default"

        return cls.create(
            database=table.project.name,
            schema=schema,
            identifier=identifier,
            type=RelationType.View if table.is_virtual_view else RelationType.Table,
        )
