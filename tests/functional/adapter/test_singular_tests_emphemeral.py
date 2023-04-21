import pytest
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.files import (
    config_materialized_ephemeral,
    schema_base_yml,
)


model_ephemeral = """
select id, name from {{ ref('base') }} where id is not null
"""

test_ephemeral_passing_sql = """
select name, id from {{ ref('ephemeral') }} where id > 1000
"""

test_ephemeral_failing_sql = """
select name, id from {{ ref('ephemeral') }} where id < 1000
"""


ephemeral_sql = config_materialized_ephemeral + model_ephemeral


@pytest.skip("TODO", allow_module_level=True)
class TestSingularTestsEphemeralODPS(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ephemeral.sql": ephemeral_sql,
            "passing_model.sql": test_ephemeral_passing_sql,
            "failing_model.sql": test_ephemeral_failing_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "passing.sql": test_ephemeral_passing_sql,
            "failing.sql": test_ephemeral_failing_sql,
        }
