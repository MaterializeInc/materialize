from dbt.adapters.materialize.connections import MaterializeConnectionManager
from dbt.adapters.materialize.connections import MaterializeCredentials
from dbt.adapters.materialize.impl import MaterializeAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import materialize

Plugin = AdapterPlugin(
    adapter=MaterializeAdapter,
    credentials=MaterializeCredentials,
    include_path=materialize.PACKAGE_PATH)
