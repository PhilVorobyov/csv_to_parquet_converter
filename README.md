# CSV to Parquet converter

This is test project that download already uploaded csv file from hdfs and convert it to parquet format

## Getting started


```shell
```

### Building

```shell
```

## Configuration

--logging.level.root
--webhdfs.uri
--remote.csv.file.path
--csv.file.path
--csv.schema.file.path
--parquet.file.path

This is the full list of all configuration that user could use during start of jar project

#### logging.level.root
Type: `String`  
Default: `'info'`

Set the level of logging ,could be used any of the next list { trace, debug, info, warn, error }

Example:
```bash
java -jar hdfs.jar --logging.level.root={logging_level}
```

#### webhdfs.uri
Type: `String`  
Default: `'webhdfs://localhost:50070'`

Set hdfs web host and port

Example:
```bash
java -jar hdfs.jar --webhdfs.uri={your_host_name:your_port}
```

#### remote.csv.file.path
Type: `String`  
Default: `'/user/maria_dev/destinations.csv'`

Download requred csv file from hdfs 

Example:
```bash
java -jar hdfs.jar --remote.csv.file.path={your_remote_csv_file_path}
```

#### csv.file.path
Type: `String`  
Default: `'/Users/philipvorobyov/devei/workspace/hdfs/src/hdfs/input_data/destinations.csv'`

Local csv file location

Example:
```bash
java -jar hdfs.jar --csv.file.path={local_csv_file_location}
```

#### csv.schema.file.path
Type: `String`  
Default: `'/Users/philipvorobyov/devei/workspace/hdfs/src/hdfs/input_data/destinations.schema'`

use requred csv file schema

Example:
```bash
java -jar hdfs.jar --csv.schema.file.path={csv_schema_local_path}
```

#### parquet.file.path
Type: `String`  
Default: `'/Users/philipvorobyov/devei/workspace/hdfs/src/hdfs/output_data/dest.parquet'`

path of final parquet file 

Example:
```bash
java -jar hdfs.jar --parquet.file.path={required_parquet_path}
```
