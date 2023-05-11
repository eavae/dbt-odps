import pytest
from dbt.tests.util import run_dbt, relation_from_name
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols, check_relation_rows
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    seeds_added_csv,
    seeds_newcolumns_csv,
)
from tests.functional.adapter.files import (
    seeds_all_added_1hour_csv,
    seeds_name_updated_csv,
    seeds_base_newcolumns_name_updated_csv,
)


class TestSnapshotCheckColsODPS(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "snapshot_strategy_check_cols",
            "snapshots": {"+properties": {"transactional": "true"}},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "added_1hour.csv": seeds_all_added_1hour_csv,
            "name_updated.csv": seeds_name_updated_csv,
        }

    def test_snapshot_check_cols(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 4

        # snapshot command
        results = run_dbt(["snapshot"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 10)
        check_relation_rows(project, "cc_name_snapshot", 10)
        check_relation_rows(project, "cc_date_snapshot", 10)

        relation = relation_from_name(project.adapter, "cc_all_snapshot")
        result = project.run_sql(f"select * from {relation}", fetch="all")

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 20)
        check_relation_rows(project, "cc_name_snapshot", 20)
        check_relation_rows(project, "cc_date_snapshot", 20)

        # re-run snapshots, using "added'
        results = run_dbt(["snapshot", "--vars", "seed_name: added_1hour"])
        for result in results:
            assert result.status == "success"

        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 30)
        check_relation_rows(project, "cc_date_snapshot", 30)
        # unchanged: only the timestamp changed
        check_relation_rows(project, "cc_name_snapshot", 20)

        # re-run snapshots, using "added'
        results = run_dbt(["snapshot", "--vars", "seed_name: name_updated"])
        for result in results:
            assert result.status == "success"
        # check rowcounts for all snapshots
        check_relation_rows(project, "cc_all_snapshot", 40)
        check_relation_rows(project, "cc_name_snapshot", 30)
        # does not see name updates
        check_relation_rows(project, "cc_date_snapshot", 30)


class TestSnapshotCheckColsWithColumnMutationsODPS(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "newcolumns.csv": seeds_newcolumns_csv,
            "seeds_base_newcolumns_name_updated.csv": seeds_base_newcolumns_name_updated_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "snapshot_strategy_check_cols_with_column_mutations",
            "snapshots": {"+properties": {"transactional": "true"}},
        }

    def test_snapshot_check_cols(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 3

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 3
        check_relation_rows(project, "cc_all_snapshot", 10)
        check_relation_rows(project, "cc_name_snapshot", 10)
        check_relation_rows(project, "cc_date_snapshot", 10)

        # seeds new columns
        results = run_dbt(["snapshot", "--vars", "seed_name: newcolumns"])
        assert len(results) == 3
        check_relation_rows(project, "cc_all_snapshot", 20)
        check_relation_rows(project, "cc_name_snapshot", 10)
        check_relation_rows(project, "cc_date_snapshot", 10)

        # seeds new columns with name updated
        results = run_dbt(["snapshot", "--vars", "seed_name: seeds_base_newcolumns_name_updated"])
        check_relation_rows(project, "cc_all_snapshot", 30)
        check_relation_rows(project, "cc_name_snapshot", 20)
        check_relation_rows(project, "cc_date_snapshot", 10)
