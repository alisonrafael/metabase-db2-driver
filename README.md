# Metabase DB2 Driver


DB2 for LUW (Linux, UNIX, Windows) driver for [Metabase](https://www.metabase.com).

> **Note:** Native DB2 support is not yet available in Metabase
> ([issue #1509](https://github.com/metabase/metabase/issues/1509)).
> This driver is maintained by the community as described in the
> [Metabase community drivers guide](https://www.metabase.com/docs/latest/developers-guide/community-drivers).

## Compatibility


| Metabase Version | Driver Download | 
|------------------|----------------|
| 0.62 | [1.1.62](https://github.com/alisonrafael/metabase-db2-driver/releases/download/v1.1.62/db2.metabase-driver.jar) |
| 0.51 | [1.1.51](https://github.com/alisonrafael/metabase-db2-driver/releases/download/v1.1.51/db2.metabase-driver.jar) |
| 0.46 | [1.1.46.2](https://github.com/alisonrafael/metabase-db2-driver/releases/download/v1.1.46.2/db2.metabase-driver.jar) |
| Older versions | [See releases](https://github.com/alisonrafael/metabase-db2-driver/releases) |


## Installation

1.  **Download the Metabase JAR** from [metabase.com](https://metabase.com/start/other.html)
   and run it once to generate the `plugins/` directory:
    ```bash
    java -jar /path/to/metabase/metabase.jar
    ```

2.  **Download the driver JAR** from the [releases page](https://github.com/alisonrafael/metabase-db2-driver/releases) and place it in the `plugins/` directory: `/path/to/metabase/plugins/db2.metabase-driver.jar`

3.  **Restart Metabase**:
    ```bash
    java -jar /path/to/metabase/metabase.jar
    ```

> **Tip:** If you encounter `CharConversionException` errors, start Metabase with the
> flag below. This makes the JCC driver convert invalid characters to NULL instead
> of throwing exceptions:
> ```bash
> java -Ddb2.jcc.charsetDecoderEncoder=3 -jar metabase.jar
> ```


## Building from Source

Use this if you want to modify the driver.

Both repositories must be cloned as siblings in the same parent directory:
```
/some/directory/
├── metabase-master/
└── metabase-db2-driver/
```

1. **Clone and prepare Metabase**
    ```bash
    git clone https://github.com/metabase/metabase metabase-master
    cd metabase-master
    clojure -X:deps prep
    ```

2. **Clone this driver**
    ```bash
    cd ..
    git clone https://github.com/alisonrafael/metabase-db2-driver
    ```

3. **Make your changes**, then build:
    ```bash
    cd metabase-db2-driver
    sh ./build.sh
    ```

4. **Copy the output to your plugins directory and restart Metabase**

   > **Note:** `/path/to/metabase/` here refers to the directory where you placed
   > the Metabase JAR in the Installation section, not the cloned source repository.

    ```bash
    cp /path/to/metabase-db2-driver/target/db2.metabase-driver.jar /path/to/metabase/plugins/
    java -jar /path/to/metabase/metabase.jar
    ```
## Acknowledgements

This driver exists thanks to the collaborative effort of the community in
[metabase/metabase#1509](https://github.com/metabase/metabase/issues/1509).
