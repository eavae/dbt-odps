from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsODPS(BaseSimpleMaterializations):
    pass


class TestSingularTestsODPS(BaseSingularTests):
    pass


class TestSingularTestsEphemeralODPS(BaseSingularTestsEphemeral):
    pass


class TestEmptyODPS(BaseEmpty):
    pass


class TestEphemeralODPS(BaseEphemeral):
    pass


class TestIncrementalODPS(BaseIncremental):
    pass


class TestGenericTestsODPS(BaseGenericTests):
    pass


class TestSnapshotCheckColsODPS(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampODPS(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodODPS(BaseAdapterMethod):
    pass
