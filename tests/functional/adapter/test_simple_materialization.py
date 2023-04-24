import pytest
from dbt.tests.util import (
    run_dbt,
    check_result_nodes_by_name,
    check_relation_types,
    check_relations_equal,
)
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from tests.functional.adapter.utils import relation_from_name


class TestSimpleMaterializationsODPS(BaseSimpleMaterializations):
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

    def test_base(self, project):
        # tested seed command
        results = run_dbt(["seed"])
        assert len(results) == 1

        # run command
        results = run_dbt()
        assert len(results) == 3
        check_result_nodes_by_name(results, ["view_model", "table_model", "swappable"])

        # check relation types
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # relations_equal
        check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

        # check relations in catalog
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 4
        assert len(catalog.sources) == 1

        # run_dbt changing materialized_var to view
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: view"])
        assert len(results) == 1
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "view",
        }
        check_relation_types(project.adapter, expected)

        # remove view
        relation = relation_from_name(project.adapter, "swappable", type="view")
        project.adapter.drop_relation(relation)

        # run_dbt changing materialized_var to incremental
        results = run_dbt(["run", "-m", "swappable", "--vars", "materialized_var: incremental"])
        assert len(results) == 1
        expected = {
            "base": "table",
            "view_model": "view",
            "table_model": "table",
            "swappable": "table",
        }
        check_relation_types(project.adapter, expected)
