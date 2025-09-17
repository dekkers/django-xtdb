from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from django.db import connections
from django.db.backends.base import features
from django.db.backends.base.introspection import TableInfo
from django.db.backends.postgresql import base, creation, introspection, operations, schema

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from django.core.management.color import Style
    from django.db.backends.utils import CursorWrapper
    from django.db.models import Model
    from django.db.models.fields import Field


class DatabaseFeatures(features.BaseDatabaseFeatures):
    uses_savepoints = False
    can_introspect_default = False
    can_introspect_foreign_keys = False
    supports_foreign_keys = False
    supports_column_check_constraints = False
    supports_table_check_constraints = False
    can_introspect_check_constraints = False
    supports_ignore_conflicts = False
    supports_partial_indexes = False
    supports_functions_in_partial_indexes = False
    supports_covering_indexes = False
    supports_expression_indexes = True
    uses_server_side_binding = False
    supports_unlimited_charfield = True

    # XTDB supports transactions, but differently than most other SQL databases
    # and not in a way Django expects.
    supports_transactions = False

    # This is still incomplete
    django_test_skips = {
        "Tests assumes ordering": {"many_to_one.tests.ManyToOneTests.test_create_after_prefetch"},
        "Failures that still need to be investigated": {
            "basic.tests.ConcurrentSaveTests.test_concurrent_delete_with_save"
        },
        "We fake number of changed rows": {
            "basic.tests.SelectOnSaveTests.test_select_on_save_lying_update",
            "basic.tests.SelectOnSaveTests.test_select_on_save",
        },
        "Wrong query count because of forced transaction": {
            "basic.tests.ModelInstanceCreationTests.test_save_parent_primary_with_default"
        },
        "XTDB does not support DEFAULT": {
            "basic.tests.ModelInstanceCreationTests.test_save_primary_with_db_default",
            "basic.tests.ModelInstanceCreationTests.test_save_primary_with_falsey_db_default",
            "field_defaults.tests.DefaultTests.test_both_default",
            "field_defaults.tests.DefaultTests.test_bulk_create_all_db_defaults",
            "field_defaults.tests.DefaultTests.test_bulk_create_all_db_defaults_one_field",
            "field_defaults.tests.DefaultTests.test_bulk_create_mixed_db_defaults",
            "field_defaults.tests.DefaultTests.test_bulk_create_mixed_db_defaults_function",
            "field_defaults.tests.DefaultTests.test_case_when_db_default_no_returning",
            "field_defaults.tests.DefaultTests.test_db_default_function",
            "field_defaults.tests.DefaultTests.test_field_db_defaults_refresh",
            "field_defaults.tests.DefaultTests.test_foreign_key_db_default",
            "field_defaults.tests.DefaultTests.test_full_clean",
            "field_defaults.tests.DefaultTests.test_null_db_default",
            "field_defaults.tests.DefaultTests.test_pk_db_default",
        },
        "XTDB has no server generated IDs, so we have primary key with a default": {
            "basic.tests.ModelTest.test_hash",
            "basic.tests.ModelTest.test_is_pk_unset",
            "field_deconstruction.tests.FieldDeconstructionTests.test_auto_field",
            "backends.base.test_operations.SqlFlushTests.test_execute_sql_flush_statements",
        },
        "XTDB requires primary key column to be set to _id": {"model_indexes.tests.SimpleIndexesTests.test_name_set"},
        "XTDB does not support constraints (unique, required, etc.) on data": {
            "many_to_one.tests.ManyToOneTests.test_fk_assignment_and_related_object_cache",
            "many_to_one.tests.ManyToOneTests.test_relation_unsaved",
            "get_or_create.tests.GetOrCreateTests.test_get_or_create_invalid_params",
            "get_or_create.tests.GetOrCreateTestsWithManualPKs.test_savepoint_rollback",
            "model_fields.test_booleanfield.BooleanFieldTests.test_null_default",
            "model_fields.test_filefield.FileFieldTests.test_unique_when_same_filename",
            "model_fields.test_integerfield.PositiveIntegerFieldTests.test_negative_values",
        },
        "XTDB INSERT functions as UPDATE": {
            "get_or_create.tests.UpdateOrCreateTests.test_manual_primary_key_test",
            "get_or_create.tests.GetOrCreateTestsWithManualPKs.test_create_with_duplicate_primary_key",
        },
        "XTDB does not support extracting day of week in datetime": {
            "basic.tests.ModelLookupTest.test_rich_lookup",
            "basic.tests.ModelLookupTest.test_does_not_exist",
            "basic.tests.ModelLookupTest.test_equal_lookup",
        },
        "XTDB does not support direct SQL INSERT without _id field": {
            "timezones.tests.LegacyDatabaseTests.test_cursor_execute_accepts_naive_datetime",
            "backends.tests.LastExecutedQueryTest.test_last_executed_query_dict",
            "backends.tests.LastExecutedQueryTest.test_last_executed_query_dict_overlap_keys",
            "backends.tests.BackendTestCase.test_cursor_execute_with_pyformat",
            "backends.tests.BackendTestCase.test_cursor_executemany",
        },
        "Tests that break later tests": {"datetimes.tests.DateTimesTests.test_21432"},
        "XTDB does not support order by RANDOM()": {"ordering.tests.OrderingTests.test_random_ordering"},
    }


class DatabaseIntrospection(introspection.DatabaseIntrospection):
    def get_table_list(self, cursor: CursorWrapper) -> list[TableInfo]:  # type: ignore[override]
        cursor.cursor.connection.connection.read_only = True
        with cursor.cursor.connection.connection.transaction():
            cursor.execute("SELECT tablename FROM pg_tables where schemaname = 'public'")
            return [TableInfo(row[0], "t") for row in cursor.fetchall()]


class DatabaseOperations(operations.DatabaseOperations):
    compiler_module = "django_xtdb.compiler"

    def adapt_ipaddressfield_value(self, value: str | None) -> str | None:
        return value

    def sequence_reset_sql(self, style: Style, model_list: Sequence[type[Model]]) -> list[Any]:
        return []

    def sql_flush(
        self, style: Style, tables: Sequence[str], *, reset_sequences: bool = False, allow_cascade: bool = False
    ) -> list[str]:
        return [
            "{} {} {};".format(
                style.SQL_KEYWORD("DELETE"), style.SQL_KEYWORD("FROM"), style.SQL_FIELD(self.quote_name(table))
            )
            for table in tables
        ]

    def execute_sql_flush(self, sql_list: Iterable[str]) -> None:
        self.connection.connection.read_only = False
        with self.connection.connection.transaction(), self.connection.cursor() as cursor:
            for sql in sql_list:
                cursor.execute(sql)

    def lookup_cast(self, lookup_type: str, internal_type: str | None = None) -> str:
        return "%s"

    def date_trunc_sql(self, lookup_type: str, sql: str, params: Any, tzname: str | None = None) -> tuple[str, Any]:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)  # type: ignore[attr-defined]
        return f"DATE_TRUNC({lookup_type.upper()}, {sql})", (*params,)

    def datetime_trunc_sql(self, lookup_type: str, sql: str, params: Any, tzname: str | None) -> str:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)  # type: ignore[attr-defined]
        return f"DATE_TRUNC({lookup_type.upper()}, {sql})"


class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):
    def create_model(self, model: type[Model]) -> None:
        return

    def alter_unique_together(
        self,
        model: type[Model],
        old_unique_together: Sequence[Sequence[str]],
        new_unique_together: Sequence[Sequence[str]],
    ) -> None:
        return

    def add_field(self, model: type[Model], field: Field[Any, Any]) -> None:
        return

    def remove_field(self, model: type[Model], field: Field[Any, Any]) -> None:
        return

    def alter_field(
        self, model: type[Model], old_field: Field[Any, Any], new_field: Field[Any, Any], strict: bool = False
    ) -> None:
        return


class DatabaseCreation(creation.DatabaseCreation):
    def _execute_create_test_db(self, cursor: Any, parameters: dict[str, Any], keepdb: bool = False) -> None:
        return

    def _destroy_test_db(self, test_database_name: str, verbosity: int) -> None:
        return


class DatabaseWrapper(base.DatabaseWrapper):
    vendor = "xtdb"
    display_name = "XTDB"
    SchemaEditorClass = DatabaseSchemaEditor
    creation_class = DatabaseCreation
    features_class: type[DatabaseFeatures] = DatabaseFeatures  # type: ignore[assignment]
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def ensure_timezone(self) -> bool:
        return False

    def _configure_connection(self, connection: Any) -> bool:
        return False

    def check_constraints(self, table_names: list[str] | None = None) -> None:
        pass

    def create_cursor(self, name: str | None = None) -> Any:
        return self.connection.cursor()

    @contextmanager
    def _nodb_cursor(self) -> Iterator[Any]:
        """
        Return a cursor that doesn't connect to a specific database.

        With XTDB we can never connect to the 'postgres' database, so we will
        connect to the first configured XTDB database.
        """
        for connection in connections.all():
            if connection.vendor == "xtdb":
                conn = self.__class__(
                    {**self.settings_dict, "NAME": connection.settings_dict["NAME"]}, alias=self.alias
                )
                try:
                    with conn.cursor() as cursor:
                        yield cursor
                finally:
                    conn.close()
                break
