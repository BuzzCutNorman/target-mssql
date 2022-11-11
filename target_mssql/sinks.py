"""mssql target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Any, Dict, cast, Iterable, Optional

from sqlalchemy import Table, MetaData, exc, types, engine_from_config, insert
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import URL, Engine
from sqlalchemy.sql.expression import Insert


from singer_sdk.connectors import SQLConnector
from singer_sdk.sinks import SQLSink


class mssqlConnector(SQLConnector):
    """The connector for mssql.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Generates a SQLAlchemy URL for mssql.

        Args:
            config: The configuration for the connector.
        """
        if config['dialect'] == "mssql":
            url_drivername:str = config['dialect']
        else:
            cls.logger.error("Invalid dialect given")
            exit(1)

        if config['driver_type'] in ["pyodbc"]:
            url_drivername += f"+{config['driver_type']}"
        else:
            cls.logger.error("Invalid driver_type given")
            exit(1)

        config_url = URL.create(
            url_drivername,
            config['user'],
            config['password'],
            host = config['host'],
            database = config['database']
        )

        if 'port' in config:
            config_url.set(port=config['port'])
        
        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(config['sqlalchemy_url_query'])
        
        return (config_url)

    def create_sqlalchemy_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {f"{eng_prefix}url":self.sqlalchemy_url,f"{eng_prefix}echo":"False"}

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return engine_from_config(eng_config, prefix=eng_prefix)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.
        
        Developers may optionally add custom logic before calling the default implementation
        inherited from the base class.
        """
        # Optionally, add custom logic before calling the super().
        # You may delete this method if overrides are not needed.
        # import logging
        # logger = logging.getLogger("sqlconnector")
        # logger.info(jsonschema_type)
        if 'boolean' in jsonschema_type.get('type'):
            return cast(types.TypeEngine, mssql.VARCHAR())
        # logger = logging.getLogger("sqlconnector")
        # logger.info(jsonschema_type)

        return SQLConnector.to_sql_type(jsonschema_type)

class mssqlSink(SQLSink):
    """mssql target sink class."""

    connector_class = mssqlConnector

    @property
    def schema_name(self) -> Optional[str]:
        """Return the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """
        stream_schema = super().schema_name

        # MS SQL Server has a public database role so the name is reserved
        # and it can not be created as a schema.  To avoid this common error
        # we convert "public" to "dbo" if the target dialet is mssql
        # if self.connector._dialect.name == "mssql" and stream_schema == "public":
        if stream_schema == "public":
            stream_schema = "dbo"
        
        return stream_schema


    def generate_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> Insert:
        """Generate an insert statement for the given records.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.

        Returns:
            An insert statement.
        """
        
        statement = insert(self.connector.get_table(full_table_name))

        return statement
    
    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        primary_key_present = False
        # #pftn -> Parsed Full Table Name [0] = db, [1] = schema, [2] = table
        # pftn:tuple = SQLConnector.parse_full_table_name(self, full_table_name=full_table_name)
        # table_name:str = pftn[2]
        _, schema_name, table_name = SQLConnector.parse_full_table_name(self, full_table_name=full_table_name)

        conformed_records = (
            [self.conform_record(record) for record in records]
            if isinstance(records, list)
            else (self.conform_record(record) for record in records)
        )

        meta = MetaData()
        # if self.schema_name:
        #     table = Table(table_name, meta, autoload=True, autoload_with=self.connector.connection.engine, schema=self.schema_name)
        # else:
        #     table = Table(table_name, meta, autoload=True, autoload_with=self.connector.connection.engine)
        table = Table(table_name, meta, autoload=True, autoload_with=self.connector.connection.engine, schema=schema_name)
        primary_key_list = [pk_column.name for pk_column in table.primary_key.columns.values()]
        for primary_key in primary_key_list:
            if primary_key in conformed_records[0]:
                primary_key_present = True
        
        insert_sql: Insert = self.generate_insert_statement(
            full_table_name,
            schema,
        )

        conformed_records = (
            [self.conform_record(record) for record in records]
            if isinstance(records, list)
            else (self.conform_record(record) for record in records)
        )

        try:
            with self.connector.connection.engine.connect() as conn:
                if primary_key_present:
                    conn.execute(f"SET IDENTITY_INSERT { full_table_name } ON")
                
                with conn.begin():
                    conn.execute(
                        insert_sql,
                        conformed_records,
                    )

                if primary_key_present:
                    conn.execute(f"SET IDENTITY_INSERT { full_table_name } OFF")
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.