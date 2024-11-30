<br/>
<p align="center">
  <a href="https://pg-gsheets.com">
    <img src="https://mintlify.s3-us-west-1.amazonaws.com/pg-gsheets/logo/pg-gsheets.svg" alt="pg-gsheets logo" />
  </a>
</p>
<br/>

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://github.com/MuhammadTahaNaveed/pg-gsheets/blob/master/LICENSE)

[pg-gsheets](https://pg-gsheets.com) is a PostgreSQL extension that enables interaction with Google Sheets directly from your PostgreSQL database. You can use SQL commands to read and write Google Sheets data.

## Installation

### Prerequisites

#### Debian

```bash
apt install make gcc libcurl4-openssl-dev postgresql-server-dev-[pg-version]
```

#### RHEL

```bash
dnf install make gcc libcurl-devel redhat-rpm-config postgresql[pg-version]-devel
```

### Install pg-gsheets

Clone or download the source code zip. Run the following command in the source code directory to build and install the extension.

```bash
make install
```

Check if the pg_config utility is in your PATH by running pg_config. If it’s not found, provide the path:

```bash
make PG_CONFIG=/path/to/postgres/bin/pg_config install
```

### Usage

#### Load the extension

Connect to the database and run the following command to load the extension:

```sql
CREATE EXTENSION IF NOT EXISTS gsheets;
```

#### Authenticate

To interact with Google Sheets, you need to authenticate your PostgreSQL environment with Google. Run the following command to authenticate:

```sql
SELECT gsheets_auth();
```

This command opens a URL in your browser. After getting the token, use the following command to set the token:

```sql
SET gsheets.access_token='your_access_token';
```

#### Read data

Following is the function signature to read data from Google Sheets:

```sql
read_sheet(spreadsheet_id/url text,
           sheet_name DEFAULT 'Sheet1',
           header boolean DEFAULT true);
```

Here’s an example of reading data from a Google Sheet:

```sql
SELECT * FROM
    read_sheet('<spreadsheet_id/url>')
as (name text, age int);
```

```sql
SELECT * FROM
read_sheet('<spreadsheet_id/url>',
           sheet_name=>'Sheet2',
           header=>false);
as (name text, age int);
```

#### Write data

Following is the function signature to write data to Google Sheets:

```sql
write_sheet(data anyelement,
            options jsonb DEFAULT '{}');
```

Available options are:
```json
{
  "spreadsheet_id": "string",   -- Optional. If not provided, a new spreadsheet is created
  "sheet_name": "string",       -- Optional. Default is 'Sheet1'
  "header": "array"             -- Optional. Default is []
}
```

Here’s an example of writing data to a Google Sheet:

```sql
SELECT write_sheet((name, age)) FROM person;
```
```sql
SELECT write_sheet(t.*) FROM person t;
```
```sql
SELECT write_sheet(name) FROM person;
```
```sql
SELECT write_sheet((name, age),
                  '{"spreadsheet_id": "<spreadsheet_id>",
                    "sheet_name": "Sheet2",
                    "header": ["name", "age"]}'::jsonb)
FROM person;
```

### Support
If you encounter any issues or have suggestions for improvements, please file an [issue](https://github.com/MuhammadTahaNaveed/pg-gsheets/issues) or contribute directly through [pull requests](https://github.com/MuhammadTahaNaveed/pg-gsheets/pulls).
