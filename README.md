


# Metabase Driver: DB2

DB2 Driver for Metabase v0.45.x, working with DB2 9.7 for LUW (Linux, UNIX, Windows) with no use of DB2_COMPATIBILITY_VECTOR and no DB2_DEFERRED_PREPARE_SEMANTICS. 

###  Running Metabase application with DB2 driver plugin
First download Metabase .jar file [here](https://metabase.com/start/other.html)  and run
```bash
java -jar metabase.jar
```
The `plugins/` directory will be created. Drop the driver in your `plugins/` directory and run metabase again. You can grab it [here](https://github.com/alisonrafael/metabase-db2-driver/releases/download/v1.1.44/db2.metabase-driver.jar) or build it yourself:

##  Editing the plugin: Prerequisites

### Java JDK 11
Check java, javac and jar installation
```bash
java -version
javac -version
jar
```

### Node.js
Install [Node.js](https://nodejs.org/)
```bash
sudo apt-get install curl
curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
node -v 
```

### Clojure
Install [Clojure](https://clojure.org/guides/getting_started)
```bash
curl -O https://download.clojure.org/install/linux-install-1.11.1.1182.sh 
chmod +x linux-install-1.11.1.1182.sh 
sudo ./linux-install-1.11.1.1182.sh
```

### Yarn
Install [Yarn](https://yarnpkg.com/lang/en/)
```bash
curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
sudo apt-get update && sudo apt-get install yarn
yarn --version
```

## Editing the plugin: Building the driver 

### Clone the Metabase project

Clone the [Metabase repo](https://github.com/metabase/metabase) first if you haven't already done so.

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

You can run as follows to avoid the CharConversionException exceptions. By this way, JCC converts invalid characters to NULL instead of throwing exceptions:

```bash
java -Ddb2.jcc.charsetDecoderEncoder=3 -jar metabase.jar
```

Use this additional JDBC properties for performance, uncommitted read ("dirty" read):

```bash
defaultIsolationLevel=1;readOnly=true;
```

## Thanks
Thanks to everybody here [https://github.com/metabase/metabase/issues/1509](https://github.com/metabase/metabase/issues/1509)
