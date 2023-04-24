import pytest
from dbt.tests.adapter.basic.test_incremental import BaseIncremental


class TestIncrementalODPS(BaseIncremental):
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
