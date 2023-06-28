
# Metabase Driver: DB2

DB2 for LUW (Linux, UNIX, Windows) Driver for [Metabase](https://www.metabase.com).

###  Versions
| Metabase Version | DB2 Driver | Bugs |
| --- | --- | --- |
| 0.46 | 1.1.46.2 ([jar](https://github.com/alisonrafael/metabase-db2-driver/releases/download/v1.1.46.2/db2.metabase-driver.jar)) | |
| Older versions | See [here](https://github.com/alisonrafael/metabase-db2-driver/releases) | See release details |

###  Running Metabase application with DB2 driver plugin
First download Metabase .jar file [here](https://metabase.com/start/other.html)  and run
```bash
java -jar metabase.jar
```
The `plugins/` directory will be created. Drop the driver in your `plugins/` directory and run metabase again. You can grab it [here](https://github.com/alisonrafael/metabase-db2-driver/releases) or build it yourself:

## Building the DB2 Driver Yourself

### Prerequisites
- Java JDK 11
- Node.js
- Clojure
- Yarn

### Clone the Metabase project

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

Inside `/metabase_source` run "clojure -X:deps prep" after clone or pull.

### Clone the DB2 Metabase Driver

Clone this [DB2 driver repo](https://github.com/alisonrafael/metabase-db2-driver) inside drivers modules folder `/metabase_source/modules/drivers` and rename this repo folder to 'db2' only.

Edit `/metabase_source/modules/drivers/deps.edn` and insert a db2 parameter, just like others: `metabase/db2 {:local/root "db2"}`.

Edit the driver as you want.

### Compile the DB2 driver

Inside `/metabase_source` run 

```bash
./bin/build-driver.sh db2
```

### Copy it to your plugins dir
```bash
mkdir -p /path/to/metabase/plugins/
cp /metabase_source/resources/modules/db2.metabase-driver.jar /path/to/metabase/plugins/
```

### Run Metabase

```bash
jar -jar /path/to/metabase/metabase.jar
```

## Configurations

Run as follows to avoid the CharConversionException exceptions. In this way, JCC converts invalid characters to NULL instead of throwing exceptions:

```bash
java -Ddb2.jcc.charsetDecoderEncoder=3 -jar metabase.jar
```

## Thanks
Thanks to everybody here [https://github.com/metabase/metabase/issues/1509](https://github.com/metabase/metabase/issues/1509)
