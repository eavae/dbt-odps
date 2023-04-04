from dbt.adapters.odps.connections import ODPSConnectionManager  # noqa
from dbt.adapters.odps.connections import ODPSCredentials
from dbt.adapters.odps.impl import ODPSAdapter
from dbt.adapters.odps.constants import PACKAGE_PATH

from dbt.adapters.base import AdapterPlugin


Plugin = AdapterPlugin(
    adapter=ODPSAdapter,
    credentials=ODPSCredentials,
    include_path=PACKAGE_PATH,
)
