"""mssql target class."""

from __future__ import annotations

from singer_sdk.target_base import SQLTarget
from singer_sdk.connectors import SQLConnector
from singer_sdk import typing as th

from target_mssql.sinks import (
    mssqlSink,
    mssqlConnector
)


class Targetmssql(SQLTarget):
    """Sample target for mssql."""

    name = "target-mssql"
    default_sink_class = mssqlSink
    default_connector_class = mssqlConnector
    _target_connector: SQLConnector = None

    @property
    def target_connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        if self._target_connector is None:
            self._target_connector = self.default_connector_class(dict(self.config))
        return self._target_connector

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchamey"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver you will be using to connect to the SQL server"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the Host serving out the SQL Instance"
        ),
        th.Property(
            "port",
            th.IntegerType,
            description="The port on which SQL awaiting connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User Account who has been granted access to the SQL Server"
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account"
        ),
        th.Property(
            "database",
            th.StringType,
            description="The Default database for this connection"
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="The Default schema to place all streams"
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                    "fast_executemany",
                    th.StringType,
                    description="Fast Executemany Mode: True, False"
                ),
                th.Property(
                    "future",
                    th.StringType,
                    description="Run the engine in 2.0 mode: True, False"
                )
            ),
            description="SQLAlchemy Engine Paramaters: fast_executemany, future"
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                    "driver",
                    th.StringType,
                    description="The Driver to use when connection should match the Driver Type"
                ),
                th.Property(
                    "TrustServerCertificate",
                    th.StringType,
                    description="This is a Yes No option"
                )
            ),
            description="SQLAlchemy URL Query options: driver, TrustServerCertificate"
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property( 
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently the only format is jsonl",
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently the only compression options is gzip",
                        )
                    )
                ),
                    th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description="the directory you want batch messages to be placed in\n"\
                                        "example: file://test/batches",
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description="What prefix you want your messages to have\n"\
                                        "example: test-batch-",
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "hd_jsonschema_types",
            th.BooleanType,
            default=False,
            description="Turn on translation of Higher Defined(HD) JSON Schema types to SQL Types"
        ),
    ).to_dict()

    def add_sqlsink(
        self,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None = None,
    ):
        """Create a sink and register it.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Primary key of the stream.

        Returns:
            A new sink for the stream.
        """
        self.logger.info("Initializing '%s' target sink...", self.name)
        sink_class = self.default_sink_class
        sink = sink_class(
            target=self,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
            connector=self.target_connector,
        )
        sink.setup()
        self._sinks_active[stream_name] = sink
        return sink
    
    def get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: list[str] | None = None,
    ):
        """Return a sink for the given stream name.

        A new sink will be created if `schema` is provided and if either `schema` or
        `key_properties` has changed. If so, the old sink becomes archived and held
        until the next drain_all() operation.

        Developers only need to override this method if they want to provide a different
        sink depending on the values within the `record` object. Otherwise, please see
        `default_sink_class` property and/or the `get_sink_class()` method.

        Raises :class:`singer_sdk.exceptions.RecordsWithoutSchemaException` if sink does
        not exist and schema is not sent.

        Args:
            stream_name: Name of the stream.
            record: Record being processed.
            schema: Stream schema.
            key_properties: Primary key of the stream.

        Returns:
            The sink used for this target.
        """
        _ = record  # Custom implementations may use record in sink selection.
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sqlsink(stream_name, schema, key_properties)

        if (
            existing_sink.schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                "Schema or key properties for '%s' stream have changed. "
                "Initializing a new '%s' sink...",
                stream_name,
                stream_name,
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sqlsink(stream_name, schema, key_properties)

        return existing_sink

if __name__ == "__main__":
    Targetmssql.cli()
