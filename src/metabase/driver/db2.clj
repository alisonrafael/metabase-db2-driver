(ns metabase.driver.db2
  "Driver for DB2 for LUW databases."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as str]
            [clj-time
             [coerce :as tcoerce]
             [core :as tc]
             [format :as time]]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.impl :as driver.impl]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [common :as sql-jdbc.common]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.util
             [date-2 :as du]
             [honey-sql-2 :as h2x]
             [ssh :as ssh]
             [log :as log]
             [i18n :refer [trs]]]
            [metabase.driver.sql :as sql]
            [schema.core :as s])
  (:import [java.sql ResultSet Types]
           java.util.Date)
  (:import java.sql.Time
           [java.util Date UUID])
  (:import (java.sql ResultSet ResultSetMetaData Time Timestamp Types)
           (java.util Calendar Date TimeZone)
           (java.time Instant LocalDateTime OffsetDateTime OffsetTime ZonedDateTime LocalDate LocalTime)
           (java.time.temporal Temporal)
           org.joda.time.format.DateTimeFormatter))

(driver/register! :db2, :parent :sql-jdbc)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :db2 [_] "DB2")

(defmethod driver/humanize-connection-error-message :db2
  [_ message]
  (condp re-matches message
    #"^FATAL: database \".*\" does not exist$"
    :database-name-incorrect

    #"^No suitable driver found for.*$"
    :invalid-hostname

    #"^Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.$"
    :cannot-connect-check-host-and-port

    #"^FATAL: role \".*\" does not exist$"
    :username-incorrect

    #"^FATAL: password authentication failed for user.*$"
    :password-incorrect

    #"^FATAL: .*$" ; all other FATAL messages: strip off the 'FATAL' part, capitalize, and add a period
    (let [[_ message] (re-matches #"^FATAL: (.*$)" message)]
      (str (str/capitalize message) \.))

    message))

;; Additional options: https://www.ibm.com/support/knowledgecenter/en/SSEPGG_9.7.0/com.ibm.db2.luw.apdv.java.doc/src/tpc/imjcc_r0052038.html
;;(defmethod driver/connection-properties :db2 [_]
;;  (ssh/with-tunnel-config
;;    [driver.common/default-host-details
;;     driver.common/default-port-details
;;     driver.common/default-dbname-details
;;     driver.common/default-user-details
;;     driver.common/default-password-details
;;     driver.common/default-ssl-details
;;     driver.common/default-additional-options-details]))

;; Needs improvements and tests
(defmethod driver.common/current-db-time-date-formatters :db2 [_]
  (mapcat
   driver.common/create-db-time-formatters
   ["yyyy-MM-dd HH:mm:ss"
    "yyyy-MM-dd HH:mm:ss.SSS"
    "yyyy-MM-dd HH:mm:ss.SSSSS"
    "yyyy-MM-dd'T'HH:mm:ss.SSS"
    "yyyy-MM-dd HH:mm:ss.SSSZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    "yyyy-MM-dd HH:mm:ss.SSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZZ"]))

(defmethod driver.common/current-db-time-native-query :db2 [_]
  "SELECT TO_CHAR(CURRENT TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') FROM SYSIBM.SYSDUMMY1") 

(defmethod driver/current-db-time :db2 [& args]
  (apply driver.common/current-db-time args))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/honey-sql-version :db2
  [_driver]
  2)

;; Wrap a HoneySQL datetime EXPRession in appropriate forms to cast/bucket it as UNIT.
;; See [this page](https://www.ibm.com/developerworks/data/library/techarticle/0211yip/0211yip3.html) for details on the functions we're using.
(defmethod sql.qp/date [:db2 :default]        [_ _ expr] expr)
(defmethod sql.qp/date [:db2 :minute]         [_ _ expr] (::h2x/extract :minute expr))
(defmethod sql.qp/date [:db2 :minute-of-hour] [_ _ expr] (::h2x/extract :minute expr))
(defmethod sql.qp/date [:db2 :hour]           [_ _ expr] (::h2x/extract :hour expr))
(defmethod sql.qp/date [:db2 :hour-of-day]    [_ _ expr] (::h2x/extract :hour expr))
(defmethod sql.qp/date [:db2 :day]            [_ _ expr] (::h2x/extract :date expr))
(defmethod sql.qp/date [:db2 :day-of-month]   [_ _ expr] (::h2x/extract :day expr))
(defmethod sql.qp/date [:db2 :week]           [_ _ expr] (::h2x/extract :week expr))
(defmethod sql.qp/date [:db2 :month]          [_ _ expr] (::h2x/extract :month expr))
(defmethod sql.qp/date [:db2 :month-of-year]  [_ _ expr] (::h2x/extract :month expr))
(defmethod sql.qp/date [:db2 :quarter]        [_ _ expr] (::h2x/extract :quarter expr))
(defmethod sql.qp/date [:db2 :year]           [_ _ expr] (::h2x/extract :date expr))
(defmethod sql.qp/date [:db2 :week-of-year]   [_ _ expr] (::h2x/extract :week expr))
(defmethod sql.qp/date [:db2 :day-of-week]     [driver _ expr] (::h2x/extract :dayofweek expr))
(defmethod sql.qp/date [:db2 :day-of-year]     [driver _ expr] (::h2x/extract :dayofyear expr))
(defmethod sql.qp/date [:db2 :quarter-of-year] [driver _ expr] (::h2x/extract :quarter expr))

(defmethod sql.qp/add-interval-honeysql-form :db2 [_ hsql-form amount unit]
  (h2x/+ (h2x/->timestamp hsql-form) (case unit
    :second  [:raw (format "%d seconds" (int amount))]
    :minute  [:raw (format "%d minutes" (int amount))]
    :hour    [:raw (format "%d hours" (int amount))]
    :day     [:raw (format "%d days" (int amount))]
    :week    [:raw (format "%d days" (int (* amount 7)))]
    :month   [:raw (format "%d months" (int amount))]
    :quarter [:raw (format "%d months" (int (* amount 3)))]
    :year    [:raw (format "%d years" (int amount))]
  )))

(defmethod sql.qp/unix-timestamp->honeysql [:db2 :seconds] [_ _ expr]
  (h2x/+ [:raw "timestamp('1970-01-01 00:00:00')"] [:raw (format "%d seconds" (int expr))])

(defmethod sql.qp/unix-timestamp->honeysql [:db2 :milliseconds] [driver _ expr]
  (h2x/+ [:raw "timestamp('1970-01-01 00:00:00')"] [:raw (format "%d seconds" (int (/ expr 1000)))])))

(def ^:private now [:raw "current timestamp"])

(defmethod sql.qp/current-datetime-honeysql-form :db2 [_] now)

(defmethod sql.qp/->honeysql [:db2 Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql.qp/->honeysql [:db2 :substring]
  [driver [_ arg start length]]
  		(if length
    	[:substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver start) [:min [:length (sql.qp/->honeysql driver arg)] (sql.qp/->honeysql driver length)]]
    	[:substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver start)]))


;; Use LIMIT OFFSET support DB2 v9.7 https://www.ibm.com/developerworks/community/blogs/SQLTips4DB2LUW/entry/limit_offset?lang=en
;; Maybe it could not to be necessary with the use of DB2_COMPATIBILITY_VECTOR
(defmethod sql.qp/apply-top-level-clause [:db2 :limit]
  [_ _ honeysql-query {value :limit}]
  {:select [:*]
   ;; if `honeysql-query` doesn't have a `SELECT` clause yet (which might be the case when using a source query) fall
   ;; back to including a `SELECT *` just to make sure a valid query is produced
   :from   [(-> (merge {:select [:*]}
                       honeysql-query)
                (update :select sql.u/select-clause-deduplicate-aliases))]
   :fetch  [:raw value]})

(defmethod sql.qp/apply-top-level-clause [:db2 :page]
  [driver _ honeysql-query {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can use the single-nesting implementation for `apply-limit`
      (sql.qp/apply-top-level-clause driver :limit honeysql-query {:limit items})
      ;; if we need to do an offset we have to do double-nesting
      {:select [:*]
       :from   [{:select [:tmp.* [[:raw "ROW_NUMBER() OVER()"] :rn]]
                 :from   [[(merge {:select [:*]}
                                  honeysql-query)
                           :tmp]]}]
       :where  [:raw (format "rn BETWEEN %d AND %d" offset (+ offset items))]})))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql date workarounds                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Filtering with dates causes a -245 error. ;;v0.33.x
;; Explicit cast to timestamp when Date function is called to prevent db2 unknown parameter type.
;; Maybe it could not to be necessary with the use of DB2_DEFERRED_PREPARE_SEMANTICS
(defmethod sql.qp/->honeysql [:db2 Date]
  [_ date]
  		(h2x/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date))) ;;v0.34.x needs it?

(defmethod sql.qp/->honeysql [:db2 Timestamp]
  [_ date]
  		(h2x/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date)))


;; MEGA HACK from sqlite.clj ;;v0.34.x
;; Fix to Unrecognized JDBC type: 2014. ERRORCODE=-4228 
(defn- zero-time? [t]
  (= (t/local-time t) (t/local-time 0)))

(defmethod sql.qp/->honeysql [:db2 LocalDate]
  [_ t]
  [:date (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:db2 LocalDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    [:datetime (h2x/literal (du/format-sql t))]))

(defmethod sql.qp/->honeysql [:db2 LocalTime]
  [_ t]
  [:time (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:db2 OffsetDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    [:datetime (h2x/literal (du/format-sql t))]))

(defmethod sql.qp/->honeysql [:db2 OffsetTime]
  [_ t]
  [:time (h2x/literal (du/format-sql t))])

(defmethod sql.qp/->honeysql [:db2 ZonedDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    [:datetime (h2x/literal (du/format-sql t))]))

;; DB2 doesn't like Temporal values getting passed in as prepared statement args, so we need to convert them to
;; date literal strings instead to get things to work (fix from sqlite.clj)
(s/defmethod driver.sql/->prepared-substitution [:db2 Temporal] :- driver.sql/PreparedStatementSubstitution
  [_driver date]
  ;; for anything that's a Temporal value convert it to a yyyy-MM-dd formatted date literal string
  ;; For whatever reason the SQL generated from parameters ends up looking like `WHERE date(some_field) = ?`
  ;; sometimes so we need to use just the date rather than a full ISO-8601 string
  (sql.params.substitution/make-stmt-subs "?" [(t/format "yyyy-MM-dd" date)]))


;; (.getObject rs i LocalDate) doesn't seem to work, nor does `(.getDate)`; ;;v0.34.x
;; Fixes merged from vertica.clj e sqlite.clj. 
;; Fix to Invalid data conversion: Wrong result column type for requested conversion. ERRORCODE=-4461

(defmethod sql-jdbc.execute/read-column-thunk [:db2 Types/DATE]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        (log/tracef "(.getString rs %d) [DATE] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:db2 Types/TIME]
  [_driver ^ResultSet rs _rsmeta ^Long i]
  (fn read-time []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        (log/tracef "(.getString rs %d) [TIME] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:db2 Types/TIMESTAMP]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        (log/tracef "(.getString rs %d) [TIMESTAMP] -> %s -> %s" i s t)
        t))))

;; instead of returning a CLOB object, return the String
(defmethod sql-jdbc.execute/read-column-thunk [:db2 Types/CLOB]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (.getString rs i)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :db2 [_ {:keys [host port db dbname]
                                                           :or   {host "localhost", port 50000, dbname ""}
                                                           :as   details}]
  (-> (merge {:classname   "com.ibm.db2.jcc.DB2Driver"
              :subprotocol "db2"
              :subname     (str "//" host ":" port "/" dbname ":" )}
             (dissoc details :host :port :dbname :ssl))
      (sql-jdbc.common/handle-additional-options details, :separator-style :semicolon)))

(defmethod driver/can-connect? :db2 [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["SELECT 1 FROM SYSIBM.SYSDUMMY1"])))))))

;; Mappings for DB2 types to Metabase types.
;; See the list here: https://docs.tibco.com/pub/spc/4.0.0/doc/html/ibmdb2/ibmdb2_data_types.htm
(defmethod sql-jdbc.sync/database-type->base-type :db2 [_ database-type]
  ({:BIGINT       :type/BigInteger    
    :BINARY       :type/*             
    :BLOB         :type/*
    :BOOLEAN      :type/Boolean
    :CHAR         :type/Text
    :CLOB         :type/Text
    :DATALINK     :type/*
    :DATE         :type/Date
    :DBCLOB       :type/Text
    :DECIMAL      :type/Decimal
    :DECFLOAT     :type/Decimal
    :DOUBLE       :type/Float
    :FLOAT        :type/Float
    :GRAPHIC      :type/Text
    :INTEGER      :type/Integer
    :NUMERIC      :type/Decimal
    :REAL         :type/Float
    :ROWID        :type/*
    :SMALLINT     :type/Integer
    :TIME         :type/Time
    :TIMESTAMP    :type/DateTime
    :VARCHAR      :type/Text
    :VARGRAPHIC   :type/Text
    :XML          :type/*
    (keyword "CHAR () FOR BIT DATA")      :type/*
    (keyword "CHAR() FOR BIT DATA") :type/*
    (keyword "LONG VARCHAR")              :type/*
    (keyword "LONG VARCHAR FOR BIT DATA") :type/*
    (keyword "LONG VARGRAPHIC")           :type/*
    (keyword "VARCHAR () FOR BIT DATA")   :type/*
    (keyword "VARCHAR() FOR BIT DATA")   :type/*} database-type))

(defmethod sql-jdbc.sync/excluded-schemas :db2 [_]
  #{"QSYS"
    "QSYS2"
    "SQLJ" 
    "SYSCAT"
    "SYSFUN"
    "SYSIBM"
    "SYSIBMADM"
    "SYSIBMINTERNAL"
    "SYSIBMTS"
    "SPOOLMAIL"
    "SYSPROC"
    "SYSPUBLIC"
    "SYSTOOLS"
    "SYSSTAT"
    "QHTTPSVR"
    "QUSRSYS"})

(defmethod sql-jdbc.execute/set-timezone-sql :db2 [_]
  "SET SESSION TIME ZONE = %s")

(defn- materialized-views
  "Fetch the Materialized Views DB2 for LUW"
  [database]  
  (try (set (jdbc/query (sql-jdbc.conn/db->pooled-connection-spec database)
      ["SELECT trim(TABSCHEMA) AS \"schema\", trim(TABNAME) AS \"name\", trim(REMARKS) AS \"description\" FROM SYSCAT.TABLES ORDER BY 1, 2"]))
       (catch Throwable e
         (log/error e (trs "Failed to fetch materialized views for DB2 for LUW")))))

(defmethod driver/describe-database :db2
  [driver database]
  (-> ((get-method driver/describe-database :sql-jdbc) driver database)
      (update :tables set/union (materialized-views database))))