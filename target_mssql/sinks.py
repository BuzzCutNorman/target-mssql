"""mssql target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Any, Dict, cast, Iterable, Optional

from sqlalchemy import Table, MetaData, exc, types, insert
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import URL



from singer_sdk.sinks import SQLConnector, SQLSink


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

    def generate_insert_statement(
        self,
        full_table_name: str,
        schema: dict,
    ) -> str:
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
        meta = MetaData()
        table = Table( full_table_name, meta, autoload=True, autoload_with=self.connector.connection.engine)
        primary_key_list = [pk_column.name for pk_column in table.primary_key.columns.values()]
        for primary_key in primary_key_list:
            if primary_key in records[0]:
                primary_key_present = True
        
        insert_sql = self.generate_insert_statement(
            full_table_name,
            schema,
        )

        try:
            with self.connector.connection.engine.begin() as conn:
                if primary_key_present:
                    conn.execute(f"SET IDENTITY_INSERT { full_table_name } ON")
                conn.execute(
                    insert_sql,
                    records,
                )
                if primary_key_present:
                    conn.execute(f"SET IDENTITY_INSERT { full_table_name } OFF")
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.