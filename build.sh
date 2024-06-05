export METABASE_PATH="/mnt/Arquivos/Desenvolvimento/Metabase/metabase"
export DRIVER_PATH="/mnt/Arquivos/Desenvolvimento/Metabase/metabase-db2-driver"

cd $METABASE_PATH

clojure \
  -Sdeps "{:aliases {:db2 {:extra-deps {com.ibm.db2/jcc {:mvn/version \"11.1.4.4\"} com.metabase/db2forluw-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:db2 \
  build-drivers.build-driver/build-driver! \
  "{:driver :db2, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"