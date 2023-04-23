import pytest
from dbt.tests.util import run_sql_with_adapter


class TestConnectionWithHints:
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        dbt_profile_target.update(
            {
                "hints": {
                    "odps.sql.hive.compatible": "true",
                }
            }
        )
        return dbt_profile_target

    def test_run_sql(self, project):
        results = run_sql_with_adapter(
            project.adapter,
            "select cast((a & b) as string) from values(-9223372036854775807L, -9223372036854775792L) t(a, b);",
            fetch="one",
        )
        assert results == ["-9223372036854775808"]


class TestConnectionsWithoutHints:
    def test_run_sql(self, project):
        results = run_sql_with_adapter(
            project.adapter,
            "select cast((a & b) as string) from values(-9223372036854775807L, -9223372036854775792L) t(a, b);",
            fetch="one",
        )
        assert results == [None]
