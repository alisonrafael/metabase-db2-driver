#!/bin/bash
cd $(dirname $0) 
DRIVER_PATH=$(pwd)

cd ../metabase-master

DB2_DRIVER_VERSION=11.5.9.0

clojure \
  -Sdeps "{:aliases {:db2 {:extra-deps {com.ibm.db2/jcc {:mvn/version \"$DB2_DRIVER_VERSION\"} com.metabase/db2forluw-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:db2 \
  build-drivers.build-driver/build-driver! \
  "{:driver :db2, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"