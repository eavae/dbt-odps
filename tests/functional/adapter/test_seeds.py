import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
)
from tests.functional.adapter.utils import relation_from_name


class TestSeeds:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }

    def test_generic_tests(self, project):
        run_results = run_dbt(["seed"])
        assert run_results[0].status == "success"
        relation = relation_from_name(project.adapter, "base")
        sql_results = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert sql_results[0] == 10, "should have 10 rows"

        # test seed when run again
        run_results = run_dbt(["seed"])
        assert run_results[0].status == "success"
        relation = relation_from_name(project.adapter, "base")
        sql_results = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert sql_results[0] == 10, "should have 10 rows"

        # test seed when run again with --full-refresh
        run_results = run_dbt(["seed", "--full-refresh"])
        assert run_results[0].status == "success"
        relation = relation_from_name(project.adapter, "base")
        sql_results = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert sql_results[0] == 10, "should have 10 rows"
