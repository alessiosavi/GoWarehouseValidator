# GoWarehouseValidator

A simple CLI tool for validate the given datasets

## WHY ?

During the initial phase of the `data warehouse` creation, is necessary to be sure that the input dataset have a
standard format.

During the initial loading of the dataset, is crucial to be sure that the input data have the same format and datatype.
By this way, you can implement an ETL processing with less time, avoiding to check/validate the input dataset every time
that a problem occurs in the ETL process.

This tool aim to validate the input dataset (for now support only `csv` file) checking that the type of the various
column are the same of the one specified in the configuration file.

## HOW ?

The tool uses a configuration file like the following one:

```json
{
  // path will save all the file that are necessary to verify
  "path": [
    "s3://my-bucket-test-s3/covid.csv",
    "dataset/covid.csv"
  ],
  // path can be specified as a s3
  "_path": "s3://my-bucket/my_csv_file.csv",
  // region of the S3
  "region": "us-east-2",
  // separator of the CSV file
  "separator": ",",
  // date format as specified in strftime format.
  "date_format": "%Y-%m-%dT%H:%M:%S",
  // key-value of the csv header - type
  "validation": {
    "data": "DATE",
    "stato": "STRING",
    "ricoverati_con_sintomi": "INTEGER",
    "terapia_intensiva": "INTEGER",
    "totale_ospedalizzati": "INTEGER",
    "isolamento_domiciliare": "INTEGER",
    "totale_positivi": "INTEGER",
    "variazione_totale_positivi": "INTEGER",
    "nuovi_positivi": "INTEGER",
    "dimessi_guariti": "INTEGER",
    "deceduti": "INTEGER",
    "casi_da_sospetto_diagnostico": "INTEGER|NULLABLE",
    "casi_da_screening": "INTEGER|NULLABLE",
    "totale_casi": "INTEGER",
    "tamponi": "INTEGER",
    "casi_testati": "INTEGER|NULLABLE",
    "note": "STRING|NULLABLE",
    "ingressi_terapia_intensiva": "INTEGER|NULLABLE",
    "note_test": "STRING|NULLABLE",
    "note_casi": "STRING|NULLABLE"
  }
}
```

## Usage

```bash
./GoWarehouseValidator -conf "conf/conf.json"
```

### Using go

```bash
go get -v -u github.com/alessiosavi/GoWarehouseValidator

./GoWarehouseValidator -conf "conf/conf.json"
```
