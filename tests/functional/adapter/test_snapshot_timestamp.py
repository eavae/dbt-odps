import pytest
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp, check_relation_rows
from dbt.tests.util import run_dbt, relation_from_name
from dbt.tests.adapter.basic.files import (
    seeds_base_csv,
    seeds_newcolumns_csv,
    seeds_added_csv,
)
from tests.functional.adapter.files import (
    seeds_all_added_1hour_csv,
    seeds_name_updated_csv,
    seeds_base_newcolumns_added_1hour_csv,
)

ts_snapshot_with_partition_sql = """
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
    )}}
    select
        *,
        cast(datepart(some_date, 'yyyy') as int) as `year`
    from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
""".strip()


class TestSnapshotTimestampODPS(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "newcolumns.csv": seeds_newcolumns_csv,
            "added.csv": seeds_added_csv,
            "added_1hour.csv": seeds_all_added_1hour_csv,
            "name_updated.csv": seeds_name_updated_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "snapshot_strategy_timestamp",
            "snapshots": {"+properties": {"transactional": "true"}},
        }

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 5

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        check_relation_rows(project, "ts_snapshot", 10)

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        check_relation_rows(project, "ts_snapshot", 20)

        results = run_dbt(["snapshot", "--vars", "seed_name: added_1hour"])
        check_relation_rows(project, "ts_snapshot", 30)

        results = run_dbt(["snapshot", "--vars", "seed_name: name_updated"])
        check_relation_rows(project, "ts_snapshot", 30)


class TestSnapshotTimestampWithColumnMutationsODPS(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "newcolumns.csv": seeds_newcolumns_csv,
            "added_1hour_newcolumns.csv": seeds_base_newcolumns_added_1hour_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "snapshot_strategy_timestamp_with_column_mutations",
            "snapshots": {"+properties": {"transactional": "true"}},
        }

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 3

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        check_relation_rows(project, "ts_snapshot", 10)

        # seeds new columns, timestamp not changed, so data not updated
        results = run_dbt(["snapshot", "--vars", "seed_name: newcolumns"])
        check_relation_rows(project, "ts_snapshot", 10)

        # seeds new columns with timestamp updated
        results = run_dbt(["snapshot", "--vars", "seed_name: added_1hour_newcolumns"])
        check_relation_rows(project, "ts_snapshot", 20)


class TestSnapshotTimestampWithPartitionODPS(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self, dbt_profile_target):
        dbt_profile_target.update(
            {
                "hints": {
                    "odps.sql.allow.fullscan": "true",
                }
            }
        )
        return dbt_profile_target

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
            "added_1hour.csv": seeds_all_added_1hour_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot_with_partition.sql": ts_snapshot_with_partition_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "snapshot_strategy_timestamp_with_partition",
            "snapshots": {
                "+properties": {"transactional": "true"},
                "+partitioned_by": [
                    {
                        "col_name": "year",
                        "data_type": "int",
                    }
                ],
            },
        }

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 3

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "ts_snapshot")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where `year`=1976", fetch="one"
        )
        assert result[0] == 2, "snapshot should have 2 rows with `year`=1976"

        # snapshot added
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "ts_snapshot")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where `year`=2009", fetch="one"
        )
        assert result[0] == 2, "snapshot should have 2 rows with `year`=2009"

        # seeds add 1hour, timestamp not changed, so data not updated
        results = run_dbt(["snapshot", "--vars", "seed_name: added_1hour"])
        assert len(results) == 1
        relation = relation_from_name(project.adapter, "ts_snapshot")
        result = project.run_sql(
            f"select count(*) as num_rows from {relation} where `year`=2009", fetch="one"
        )
        assert result[0] == 4, "snapshot should have 4 rows with `year`=2009"
