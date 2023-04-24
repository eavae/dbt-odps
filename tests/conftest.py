import pytest
import os
from dbt.exceptions import CompilationError, DbtDatabaseError
from dbt.events.functions import setup_event_logger, cleanup_event_logger
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import get_connection

pytest_plugins = ["dbt.tests.fixtures.project"]


class OdpsTestProjInfo(TestProjInfo):
    def create_test_schema(self, schema_name=None):
        pass

    def drop_test_schema(self, schema_name=None):
        with get_connection(self.adapter):
            relations = self.adapter.list_relations_without_caching()
            for relation in relations:
                self.adapter.drop_relation(relation)


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return "default"


@pytest.fixture(scope="class")
def project(
    clean_up_logging,
    project_root,
    profiles_root,
    request,
    unique_schema,
    profiles_yml,
    dbt_project_yml,
    packages_yml,
    selectors_yml,
    adapter,
    project_files,
    shared_data_dir,
    test_data_dir,
    logs_dir,
    test_config,
):
    setup_event_logger(logs_dir)
    orig_cwd = os.getcwd()
    os.chdir(project_root)
    # Return whatever is needed later in tests but can only come from fixtures, so we can keep
    # the signatures in the test signature to a minimum.
    project = OdpsTestProjInfo(
        project_root=project_root,
        profiles_dir=profiles_root,
        adapter_type=adapter.type(),
        test_dir=request.fspath.dirname,
        shared_data_dir=shared_data_dir,
        test_data_dir=test_data_dir,
        test_schema=unique_schema,
        database=adapter.config.credentials.database,
        test_config=test_config,
    )
    project.drop_test_schema()
    project.create_test_schema()

    yield project

    try:
        project.drop_test_schema()
    except (KeyError, AttributeError, CompilationError, DbtDatabaseError):
        pass
    os.chdir(orig_cwd)
    cleanup_event_logger()


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "odps",
        "endpoint": os.environ.get("ODPS_ENDPOINT"),
        "access_id": os.getenv("ODPS_ACCESS_ID"),
        "secret_access_key": os.getenv("ODPS_SECRET_ACCESS_KEY"),
        "database": os.environ.get("ODPS_PROJECT"),
        "schema": "public",
    }
