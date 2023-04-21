"""mssql target sink class, which handles writing streams."""

from __future__ import annotations
from typing import Any, Dict, cast, Iterable, Optional

import pyodbc

import sqlalchemy
from sqlalchemy import DDL, Table, MetaData, exc, types, engine_from_config
from sqlalchemy.dialects import mssql
from sqlalchemy.engine import URL, Engine

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

    def __init__(
            self,
            config: dict | None = None,
            sqlalchemy_url: str | None = None
    ) -> None:
        # If pyodbc given set pyodbc.pooling to False
        # This allows SQLA to manage to connection pool
        if config['driver_type'] == 'pyodbc':
            pyodbc.pooling = False

        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Generates a SQLAlchemy URL for mssql.

        Args:
            config: The configuration for the connector.

        Returns:
            The URL as a string.
        """
        if config['dialect'] == "mssql":
            url_drivername: str = config['dialect']
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
            host=config['host'],
            database=config['database']
        )

        if 'port' in config:
            config_url = config_url.set(port=config['port'])

        if 'sqlalchemy_url_query' in config:
            config_url = config_url.update_query_dict(config['sqlalchemy_url_query'])

        return (config_url)

    def create_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": "False"
        }

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return engine_from_config(eng_config, prefix=eng_prefix)

    def to_sql_type(self, jsonschema_type: dict) -> None:
        """Returns a JSON Schema equivalent for the given SQL type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        if self.config.get('hd_jsonschema_types', False):
            return self.hd_to_sql_type(jsonschema_type)
        else:
            return self.org_to_sql_type(jsonschema_type)

    @staticmethod
    def org_to_sql_type(jsonschema_type: dict) -> types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """

        if 'boolean' in jsonschema_type.get('type'):
            return cast(types.TypeEngine, mssql.VARCHAR(length=5))

        return SQLConnector.to_sql_type(jsonschema_type)

    @staticmethod
    def hd_to_sql_type(jsonschema_type: dict) -> types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input
        argument types, to support non-standard types, or to provide custom
        typing logic. If overriding this method, developers should call the
        default implementation from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        # Send date, time, and date-time to specific MSSQL type
        # Strings to NVARCHAR and add maxLength
        if 'string' in jsonschema_type.get('type'):
            if jsonschema_type.get("format") == "date":
                return cast(sqlalchemy.types.TypeEngine, mssql.DATE())
            if jsonschema_type.get("format") == "time":
                return cast(sqlalchemy.types.TypeEngine, mssql.TIME())
            if jsonschema_type.get("format") == "date-time":
                return cast(sqlalchemy.types.TypeEngine, mssql.DATETIME())
            if jsonschema_type.get("format") == "uuid":
                return cast(sqlalchemy.types.TypeEngine, mssql.UNIQUEIDENTIFIER())
            if jsonschema_type.get("contentMediaType") == "application/xml":
                return cast(sqlalchemy.types.TypeEngine, mssql.XML())
            length: int = jsonschema_type.get('maxLength')
            if length:
                return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR(length=length))
            else:
                return cast(sqlalchemy.types.TypeEngine, mssql.NVARCHAR())

        # This is a MSSQL only DataType
        # SQLA does the converion Python True, False
        # to MS SQL Server BIT 0, 1
        if 'boolean' in jsonschema_type.get('type'):
            return cast(types.TypeEngine, mssql.BIT)

        # MS SQL Server Intergers and ANSI SQL Integers
        if 'integer' in jsonschema_type.get('type'):
            minimum: float = jsonschema_type.get('minimum')
            maximum: float = jsonschema_type.get('maximum')
            if (minimum == -9223372036854775808) and (maximum == 9223372036854775807):
                return cast(sqlalchemy.types.TypeEngine, mssql.BIGINT())
            elif (minimum == -2147483648) and (maximum == 2147483647):
                return cast(sqlalchemy.types.TypeEngine, mssql.INTEGER())
            elif (minimum == -32768) and (maximum == 32767):
                return cast(sqlalchemy.types.TypeEngine, mssql.SMALLINT())
            elif (minimum == 0) and (maximum == 255):
                # This is a MSSQL only DataType
                return cast(sqlalchemy.types.TypeEngine, mssql.TINYINT())
            else:
                precision = str(maximum).count('9')
                return cast(sqlalchemy.types.TypeEngine, mssql.DECIMAL(precision=precision, scale=0))

        # MS SQL Server monetary, currency, float, and real values
        if 'number' in jsonschema_type.get('type'):
            minimum = jsonschema_type.get('minimum')
            maximum = jsonschema_type.get('maximum')
            if (minimum == -922337203685477.6) and (maximum == 922337203685477.6):
            # There is something that is traucating and rounding this number
            # if (minimum == -922337203685477.5808) and (maximum == 922337203685477.5807):
                return cast(sqlalchemy.types.TypeEngine, mssql.MONEY())
            elif (minimum == -214748.3648) and (maximum == 214748.3647):
                return cast(sqlalchemy.types.TypeEngine, mssql.SMALLMONEY())
            elif (minimum == -1.79e308) and (maximum == 1.79e308):
                return cast(sqlalchemy.types.TypeEngine, mssql.FLOAT())
            elif (minimum == -3.40e38) and (maximum == 3.40e38):
                return cast(sqlalchemy.types.TypeEngine, mssql.REAL())
            else:
                # Python will start using scientific notition for float values.
                # A check for 'e+' in the string of the value is what I key off.
                # If it is no present we can count the number of '9' in the string.
                # If it is present we need to do a little more parsing to translate.
                if 'e+' not in str(maximum):
                    precision = str(maximum).count('9')
                    scale = precision - str(maximum).rfind('.')
                    return cast(sqlalchemy.types.TypeEngine, mssql.DECIMAL(precision=precision, scale=scale))
                else:
                    precision_start = str(maximum).rfind('+')
                    precision = int(str(maximum)[precision_start:])
                    scale_start = str(maximum).find('.') + 1
                    scale_end = str(maximum).find('e')
                    scale = scale_end - scale_start
                    return cast(sqlalchemy.types.TypeEngine, mssql.DECIMAL(precision=precision, scale=scale))

        return SQLConnector.to_sql_type(jsonschema_type)

    def _create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Create a new column.

        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.

        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            raise NotImplementedError("Adding columns is not supported.")

        column_add_ddl = self.get_column_add_ddl(
            table_name=full_table_name,
            column_name=column_name,
            column_type=sql_type
        )
        self.raw_conn_execute(str(column_add_ddl))

    def rename_column(
            self,
            full_table_name: str,
            old_name: str,
            new_name: str
    ) -> None:
        """Rename the provided columns.

        Args:
            full_table_name: The fully qualified table name.
            old_name: The old column to be renamed.
            new_name: The new name for the column.

        Raises:
            NotImplementedError: If `self.allow_column_rename` is false.
        """
        if not self.allow_column_rename:
            raise NotImplementedError("Renaming columns is not supported.")

        column_rename_ddl = self.get_column_rename_ddl(
            table_name=full_table_name, column_name=old_name, new_column_name=new_name
        )
        self.raw_conn_execute(str(column_rename_ddl))

    def raw_conn_execute(self, sql_command: str) -> None:
        """Run direct SQL commands via SQLA raw connection

        Args:
            sql_command: The SQL you want to run

        """
        self.logger.info(sql_command)
        raw_conn = self.connection.engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            cursor.execute(sql_command)
            # fetch result parameters
            # results = list(cursor.fetchall())
            cursor.close()
            raw_conn.commit()
        finally:
            raw_conn.close()

    @staticmethod
    def get_column_rename_ddl(
        table_name: str, column_name: str, new_column_name: str
    ) -> DDL:
        """Get the create column DDL statement.

        Override this if your database uses
        a different syntax for renaming columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Existing column name.
            new_column_name: New column name.

        Returns:
            A sqlalchemy DDL instance.
        """
        full_column_name = f"'{table_name}.{column_name}'"
        new_column_name = f"'{new_column_name}'"
        object_type: str = "'COLUMN'"

        return DDL(
            "EXEC sp_rename %(full_column_name)s, %(new_column_name)s, %(object_type)s",
            {
                "full_column_name": full_column_name,
                "new_column_name": new_column_name,
                "object_type": object_type,
            },
        )

    @staticmethod
    def get_column_add_ddl(
        table_name: str, column_name: str, column_type: types.TypeEngine
    ) -> DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                column_type,
            )
        )
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ADD %(create_column_clause)s",
            {
                "table_name": table_name,
                "create_column_clause": create_column_clause,
            },
        )


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

    def conform_name(self, name: str, object_type: Optional[str] = None) -> str:
        """Conform a stream property name to one suitable for the target system.

        Transforms names to snake case by default, applicable to most common DBMSs'.
        Developers may override this method to apply custom transformations
        to database/schema/table/column names.

        Args:
            name: Property name.
            object_type: One of ``database``, ``schema``, ``table`` or ``column``.


        Returns:
            The name transformed to snake case.
        """
        # # strip non-alphanumeric characters, keeping - . _ and spaces
        # name = re.sub(r"[^a-zA-Z0-9_\-\.\s]", "", name)
        # # convert to snakecase
        # name = snakecase(name)
        # # replace leading digit
        # return replace_leading_digit(name)
        return(name)
        # return super().conform_name(name)

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
        # We need to grab the schema_name and table_name
        # for the Table class instance
        _, schema_name, table_name = SQLConnector.parse_full_table_name(self, full_table_name=full_table_name)

        # You also need a blank MetaData instance
        # for the Table class instance
        meta = MetaData()

        # This is the Table instance that will autoload
        # all the info about the table from the target server
        table = Table(table_name, meta, autoload=True, autoload_with=self.connector._engine, schema=schema_name)

        conformed_records = (
            [self.conform_record(record) for record in records]
            if isinstance(records, list)
            else (self.conform_record(record) for record in records)
        )

        # This is a insert based off SQLA example
        # https://docs.sqlalchemy.org/en/20/dialects/mssql.html#insert-behavior
        try:
            with self.connector._connect() as conn:
                with conn.begin():
                    conn.execute(
                        table.insert(),
                        conformed_records,
                    )
        except exc.SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            self.logger.info(error)

        if isinstance(records, list):
            return len(records)  # If list, we can quickly return record count.

        return None  # Unknown record count.
