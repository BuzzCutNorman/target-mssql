# target-mssql

`target-mssql` is a Singer target for mssql. !!! Warning !!! really early version.  It works ok 😐.  This target can accept Meltano Batch Messages. It fails loading streams that contain datetime 😵😢. So yah sorry about that! I will be doing a two fingered clap and putting on my thinking cap about that one 🤔.  Not doing native bcp loads yet.

Built with the [Meltano Target SDK](https://sdk.meltano.com).
### Whats New 🛳️🎉
**2023-10-16 Upgraded to Meltano Singer-SDK 0.32.0:** SQLAlchemy 2.x is main stream in this version so I took advantage of that and bumped from `1.4.x` to `2.x`.  The issue with Windows wheels for `pymssql` was resolved so I bumped it back up to `2.2.8`. In the `hd_jsonschema_types` the `minimum` and `maximum` values used to define `NUMERIC` or `DECIMAL` precision and scale values were being rounded.  This caused an issue with the translation on the target side.  I leveraged scientific notation to resolve this. Runs that contained streams with `strings` as primary keys were failing during table creation.  This has been resolved by setting all primary key columns of `string` type to a size of `450` bytes or `NVARCHAR(450)`.  

**2023-04-26 New HD JSON Schema Types:**  Added translations for HD JSON Schema definitions of Xml and Binary types from the buzzcutnorman `tap-mssql`.  This is Thanks🙏 to Singer-SDK 0.24.0 which allows for JSON Schema `contentMediaType` and `contentEncoding`.  Currently all Binary data types are decoded before being inserted as `VARBINARY`.  `XML` types do not have the Collection XML schema just the XML content.

**2023-02-08 Higher Defined(HD) JSON Schema types:**  Translates the Higher Defined(HD) JSON Schema types from the buzzcutnorman `tap-mssql` back into MS SQL data types.  You can give it a try by setting `hd_jsonschema_types` to `True` in your config.json or meltano.yml
<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.
-->
## Installation
<!--
Install from PyPi:

```bash
pipx install target-mssql
```
-->
### Prerequisites
You will need to install the SQL Server Native Driver or ODBC Driver for SQL Server

[Installing Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/windows/system-requirements-installation-and-driver-files?view=sql-server-ver16#installing-microsoft-odbc-driver-for-sql-server)

Install from GitHub:

```bash
pipx install git+https://github.com/BuzzCutNorman/target-mssql.git
```

Install using [Meltano](https://www.meltano.com) as a [Custom Plugin](https://docs.meltano.com/guide/plugin-management#custom-plugins)


## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-mssql --about --format=markdown
```
-->
| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| dialect             | False    | None    | The Dialect of SQLAlchamey |
| driver_type         | False    | None    | The Python Driver you will be using to connect to the SQL server |
| host                | False    | None    | The FQDN of the Host serving out the SQL Instance |
| port                | False    | None    | The port on which SQL awaiting connection |
| user                | False    | None    | The User Account who has been granted access to the SQL Server |
| password            | False    | None    | The Password for the User account |
| database            | False    | None    | The Default database for this connection |
| sqlalchemy_eng_params| False    | None    | SQLAlchemy Engine Paramaters: fast_executemany, future |
| sqlalchemy_url_query| False    | None    | SQLAlchemy URL Query options: driver, TrustServerCertificate |
| batch_config        | False    | None    | Optional Batch Message configuration |
| start_date          | False    | None    | The earliest record date to sync |
| hd_jsonschema_types  | False   |       0 | Turn on translation of Higher Defined(HD) JSON Schema types to SQL Types |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-mssql --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.
<!--
### Source Authentication and Authorization


Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-mssql` by itself or in a pipeline using [Meltano](https://meltano.com/).
<!--
### Executing the Target Directly

```bash
target-mssql --version
target-mssql --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-mssql --config /path/to/target-mssql-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_mssql/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-mssql` CLI interface directly using `poetry run`:

```bash
poetry run target-mssql --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->
<!--
Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-mssql
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-mssql --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-mssql
```
-->
### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
