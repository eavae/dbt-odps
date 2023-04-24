import pytest
from dbt.tests.adapter.basic.test_adapter_methods import (
    BaseAdapterMethod,
    models__expected_sql,
    models__upstream_sql,
)

models__model_sql = """
select 2 as id
"""


class TestBaseAdapterMethodODPS(BaseAdapterMethod):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        dbt_profile_target.update(
            {
                "hints": {
                    "odps.sql.allow.cartesian": "true",
                }
            }
        )
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream.sql": models__upstream_sql,
            "expected.sql": models__expected_sql,
            # TODO: revert this to test schema is working
            "model.sql": models__model_sql,
        }

    def test_adapter_methods(self, project, equal_tables):
        return super().test_adapter_methods(project, equal_tables)
