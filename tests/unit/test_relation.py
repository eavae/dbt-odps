from unittest import mock
from dbt.adapters.odps.relation import OdpsRelation


def test_render_with_default_schema():
    relation = OdpsRelation.create(
        database="db",
        schema="default",
        identifier="table",
        type="table",
    )
    assert relation.render() == "`db`.`table`"


def test_render_with_non_default_schema():
    relation = OdpsRelation.create(
        database="db",
        schema="schema",
        identifier="table",
        type="table",
    )
    assert relation.render() == "`db`.`schema`.`table`"


def test_create_from_odps_table_with_custom_schema():
    table = mock.MagicMock()
    table.name = "table"
    table.project.name = "db"
    table.is_virtual_view = False
    table.get_schema.return_value = mock.MagicMock()
    table.get_schema.return_value.name = "my_schema"

    relation = OdpsRelation.from_odps_table(table)
    assert relation.database == "db"
    assert relation.schema == "my_schema"
    assert relation.identifier == "table"
    assert relation.render() == "`db`.`my_schema`.`table`"


def test_create_from_odps_table_with_default_schema():
    table = mock.MagicMock()
    table.name = "table"
    table.project.name = "db"
    table.is_virtual_view = False
    table.get_schema.return_value = None

    relation = OdpsRelation.from_odps_table(table)
    assert relation.database == "db"
    assert relation.schema == "default"
    assert relation.identifier == "table"
    assert relation.render() == "`db`.`table`"
