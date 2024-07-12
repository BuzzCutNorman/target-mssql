# target-mssql

`target-mssql` is a Singer target for mssql. !!! Warning !!! really early version.  It works ok 😐.  This target can accept Meltano Batch Messages. It fails loading streams that contain datetime 😵😢. So yah sorry about that! I will be doing a two fingered clap and putting on my thinking cap about that one 🤔.  Not doing native bcp loads yet.

Built with the [Meltano Target SDK](https://sdk.meltano.com).
### Whats New 🛳️🎉
**2024-07-12 Upgraded to Meltano Singer-SDK 0.36.1**

**2024-01-31 Upgraded to Meltano Singer-SDK 0.34.1:** Happy New Year!!!🎉.  My goal was to start using tags and releases by 2024 and was pretty close.  You can now lock on a release number if you want. 

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
| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| dialect | True     | mssql   | The Dialect of SQLAlchamey |
| driver_type | True     | pymssql | The Python Driver you will be using to<BR/>connect to the SQL server |
| host | True     | None    | The FQDN of the Host serving out the<BR/>SQL Instance |
| port | False    | None    | The port on which SQL awaiting connection |
| user | True     | None    | The User Account who has been granted<BR/>access to the SQL Server |
| password | True     | None    | The Password for the User account |
| database | True     | None    | The Default database for this connection |
| default_target_schema | False    | None    | The Default schema to place all streams |
| sqlalchemy_eng_params | False    | None    | SQLAlchemy Engine Paramaters:<BR/>fast_executemany, future |
| sqlalchemy_eng_params.fast_executemany | False    | None    | Fast Executemany Mode: True, False |
| sqlalchemy_eng_params.future | False    | None    | Run the engine in 2.0 mode: True, False |
| sqlalchemy_url_query | False    | None    | SQLAlchemy URL Query options: driver, MultiSubnetFailover, TrustServerCertificate |
| sqlalchemy_url_query.driver | False    | None    | The Driver to use when connection should<BR/>match the Driver Type |
| sqlalchemy_url_query.MultiSubnetFailover | False    | None    | This is a Yes No option |
| sqlalchemy_url_query.TrustServerCertificate | False    | None    | This is a Yes No option |
| batch_config | False    | None    | Optional Batch Message configuration |
| batch_config.encoding | False    | None    |             |
| batch_config.encoding.format | False    | None    | Currently the only format is jsonl |
| batch_config.encoding.compression | False    | None    | Currently the only compression options is<BR/>gzip |
| batch_config.storage | False    | None    |             |
| batch_config.storage.root | False    | None    | The directory you want batch<BR/>messages to be placed in.<BR/>example: file://test/batches |
| batch_config.storage.prefix | False    | None    | What prefix you want your<BR/>messages to have<BR/>example: test-batch- |
| start_date | False    | None    | The earliest record date to sync |
| hd_jsonschema_types | False    |       False | Turn on translation of Higher Defined(HD)<BR/>JSON Schema types to SQL Types |
| hard_delete | False    |       False | Hard delete records. |
| add_record_metadata | False    | None    | Add metadata to records. |
| load_method | False    | append-only | The method to use when loading data into<BR/>the destination. `append-only` will always<BR/>write all input records whether that<BR/>records already exists or not. <BR/>`upsert` will update existing records and<BR/>insert new records. `overwrite` will<BR/>delete all existing records and insert all input records. |
| batch_size_rows | False    | None    | Maximum number of rows in each batch. |
| validate_records | False    |       True | Whether to validate the schema of the<BR/>incoming streams. |
| stream_maps | False    | None    | Config object for stream maps capability.<BR/>For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values to be used within map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance<BR/>variable `fake` used within map expressions.<BR/>Only applicable if the plugin specifies<BR/>`faker` as an addtional dependency<BR/>(through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker generator<BR/>for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale strings to<BR/>produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema flattening and<BR/>automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |

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
