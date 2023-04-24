import pytest
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral


class TestEphemeralODPS(BaseEphemeral):
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
