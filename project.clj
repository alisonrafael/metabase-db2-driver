(defproject metabase/db2forluw-driver "1.0.47"
  :min-lein-version "2.5.0"

  :dependencies
  [[com.ibm.db2/jcc "11.1.4.4"]]

  :profiles
  {:provided
   {:dependencies [[metabase-core "1.0.0-SNAPSHOT"]]}

   :clean-targets [:target-path "build/js/output"]

   :uberjar
   {:auto-clean    true
    :aot :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "db2forluw.jar"}})