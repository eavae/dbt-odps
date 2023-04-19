from unittest import mock
from dbt.adapters.odps.relation import OdpsRelation


def test_render_with_public_schema():
    relation = OdpsRelation.create(
        database="db",
        schema="public",
        identifier="table",
        type="table",
    )
    assert relation.render() == "`db`.`table`"


def test_render_with_non_public_schema():
    relation = OdpsRelation.create(
        database="db",
        schema="schema",
        identifier="table",
        type="table",
    )
    assert relation.render() == "`db`.`__schema__table`"


def test_create_from_odps_table_with_custom_schema():
    table = mock.MagicMock()
    table.name = "__schema__table"
    table.project.name = "db"
    table.is_virtual_view = False

    relation = OdpsRelation.from_odps_table(table)
    assert relation.database == "db"
    assert relation.schema == "schema"
    assert relation.identifier == "table"
    assert relation.render() == "`db`.`__schema__table`"


def test_create_from_odps_table_with_public_schema():
    table = mock.MagicMock()
    table.name = "table"
    table.project.name = "db"
    table.is_virtual_view = False
    table._schema_name = None

    relation = OdpsRelation.from_odps_table(table)
    assert relation.database == "db"
    assert relation.schema == "public"
    assert relation.identifier == "table"
    assert relation.render() == "`db`.`table`"
