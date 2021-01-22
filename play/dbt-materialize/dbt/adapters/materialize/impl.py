from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.postgres import PostgresColumn
from dbt.adapters.materialize import MaterializeConnectionManager

MATERIALIZE_GET_COLUMNS_MACRO_NAME = 'materialize_get_columns'
MATERIALIZE_CONVERT_COLUMNS_MACRO_NAME = 'sql_convert_columns_in_relation'
MATERIALIZE_GET_SCHEMAS_MACRO_NAME = 'materialize_get_schemas'
MATERIALIZE_GET_FULL_VIEWS_MACRO_NAME = 'materialize_get_full_views'
MATERIALIZE_SHOW_VIEW_MACRO_NAME = 'materialize_show_view'

class MaterializeAdapter(PostgresAdapter):
    ConnectionManager = MaterializeConnectionManager
    Column = PostgresColumn

    @classmethod
    def date_function(cls):
        return 'now()'

    @classmethod
    def is_cancelable(cls):
        return False

    def get_columns_in_relation(self, relation):
        columns = self.execute_macro(
            MATERIALIZE_GET_COLUMNS_MACRO_NAME,
            kwargs={'relation': relation}
        )

        table = []
        for _field, _nullable, _type in columns:
           table.append((_field, _type))
 
        return self.execute_macro(
            MATERIALIZE_CONVERT_COLUMNS_MACRO_NAME,
            kwargs={'table': table}
        )

    def list_relations_without_caching(self, schema_relation):
        # Materialize errors if you try to list views from a schema that
        # doesn't exist. Check that the schema exists first, returning an
        # empty list of relations if not.
        schemas = self.execute_macro(MATERIALIZE_GET_SCHEMAS_MACRO_NAME)
        if schema_relation.schema not in schemas:
            return []

        full_views = self.execute_macro(
            MATERIALIZE_GET_FULL_VIEWS_MACRO_NAME,
            kwargs={'schema': schema_relation.schema}
        )

        relations = []
        quote_policy = {
            'database': True,
            'schema': True,
            'identifier': True
        }
        for _view, _type, _materialized in full_views.rows:
            dbt_type = 'table' if _materialized else 'view'
            relations.append(self.Relation.create(
                  database=schema_relation.database,
                  schema=schema_relation.schema,
                  identifier=_view,
                  quote_policy=quote_policy,
                  type=dbt_type
            ))
        return relations

    def check_schema_exists(self, database, schema):
        return schema in self.list_schemas(database)

    # jwills hacking to get stuff to work
    def _link_cached_relations(self, manifest):
        schemas = set()
        # only link executable nodes
        relations_schemas = self._get_cache_schemas(manifest)
        for relation in relations_schemas:
            self.verify_database(relation.database)
            schemas.add(relation.schema.lower())
