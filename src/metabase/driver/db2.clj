(ns metabase.driver.db2
  "Driver for DB2 for LUW databases."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clj-time
             [coerce :as tcoerce]
             [core :as tc]
             [format :as time]]
            [honeysql
             [core :as hsql]
             [format :as hformat]]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [common :as sql-jdbc.common]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.util
             [date-2 :as du]
             [honeysql-extensions :as hx]
             [ssh :as ssh]]
            [metabase.driver.sql :as sql]
            [metabase.query-processor.timezone :as qp.timezone]
            [schema.core :as s])
  (:import [java.sql ResultSet Types]
           java.util.Date)
  (:import java.sql.Time
           [java.util Date UUID])
  (:import [java.sql ResultSet Time Timestamp Types]
           [java.util Calendar Date TimeZone]
           [java.time Instant LocalDateTime OffsetDateTime OffsetTime ZonedDateTime LocalDate LocalTime]
           metabase.util.honeysql_extensions.Literal
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

(defn- date-format [format-str expr] (hsql/call :varchar_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))
(defn- trunc-with-format [format-str expr](str-to-date format-str (date-format format-str expr)))

;; Wrap a HoneySQL datetime EXPRession in appropriate forms to cast/bucket it as UNIT.
;; See [this page](https://www.ibm.com/developerworks/data/library/techarticle/0211yip/0211yip3.html) for details on the functions we're using.
(defmethod sql.qp/date [:db2 :default]        [_ _ expr] expr)
(defmethod sql.qp/date [:db2 :minute]         [_ _ expr] (trunc-with-format "YYYY-MM-DD HH24:MI" expr))
(defmethod sql.qp/date [:db2 :minute-of-hour] [_ _ expr] (hsql/call :minute expr))
(defmethod sql.qp/date [:db2 :hour]           [_ _ expr] (trunc-with-format "YYYY-MM-DD HH24" expr))
(defmethod sql.qp/date [:db2 :hour-of-day]    [_ _ expr] (hsql/call :hour expr))
(defmethod sql.qp/date [:db2 :day]            [_ _ expr] (hsql/call :date expr))
(defmethod sql.qp/date [:db2 :day-of-month]   [_ _ expr] (hsql/call :day expr))
(defmethod sql.qp/date [:db2 :week]           [_ _ expr] (hx/- expr (hsql/raw (format "%d days" (int (hx/- (hsql/call :dayofweek expr) 1))))))
(defmethod sql.qp/date [:db2 :month]          [_ _ expr] (str-to-date "YYYY-MM-DD" (hx/concat (date-format "YYYY-MM" expr) (hx/literal "-01"))))
(defmethod sql.qp/date [:db2 :month-of-year]  [_ _ expr] (hsql/call :month expr))
(defmethod sql.qp/date [:db2 :quarter]        [_ _ expr] (str-to-date "YYYY-MM-DD" (hsql/raw (format "%d-%d-01" (int (hx/year expr)) (int ((hx/- (hx/* (hx/quarter expr) 3) 2)))))))
(defmethod sql.qp/date [:db2 :year]           [_ _ expr] (hsql/call :date expr))
(defmethod sql.qp/date [:db2 :week-of-year]   [_ _ expr] (hsql/call :week expr))
(defmethod sql.qp/date [:db2 :day-of-week]     [driver _ expr] (hsql/call :dayofweek expr))
(defmethod sql.qp/date [:db2 :day-of-year]     [driver _ expr] (hsql/call :dayofyear expr))
(defmethod sql.qp/date [:db2 :quarter-of-year] [driver _ expr] (hsql/call :quarter expr))

(defmethod sql.qp/add-interval-honeysql-form :db2 [_ hsql-form amount unit]
  (hx/+ (hx/->timestamp hsql-form) (case unit
    :second  (hsql/raw (format "%d seconds" (int amount)))
    :minute  (hsql/raw (format "%d minutes" (int amount)))
    :hour    (hsql/raw (format "%d hours" (int amount)))
    :day     (hsql/raw (format "%d days" (int amount)))
    :week    (hsql/raw (format "%d days" (int (hx/* amount (hsql/raw 7)))))
    :month   (hsql/raw (format "%d months" (int amount)))
    :quarter (hsql/raw (format "%d months" (int (hx/* amount (hsql/raw 3)))))
    :year    (hsql/raw (format "%d years" (int amount)))
  )))

(defmethod sql.qp/unix-timestamp->honeysql [:db2 :seconds] [_ _ expr]
  (hx/+ (hsql/raw "timestamp('1970-01-01 00:00:00')") (hsql/raw (format "%d seconds" (int expr))))

(defmethod sql.qp/unix-timestamp->honeysql [:db2 :milliseconds] [driver _ expr]
  (hx/+ (hsql/raw "timestamp('1970-01-01 00:00:00')") (hsql/raw (format "%d seconds" (int (hx// expr 1000)))))))

(def ^:private now (hsql/raw "current timestamp"))

(defmethod sql.qp/current-datetime-honeysql-form :db2 [_] now)

(defmethod sql.qp/->honeysql [:db2 Boolean]
  [_ bool]
  (if bool 1 0))

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
   :fetch  [(hsql/raw (format "FIRST %d ROWS ONLY" value))]})

(defmethod sql.qp/apply-top-level-clause [:db2 :page]
  [driver _ honeysql-query {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can use the single-nesting implementation for `apply-limit`
      (sql.qp/apply-top-level-clause driver :limit honeysql-query {:limit items})
      ;; if we need to do an offset we have to do double-nesting
      {:select [:*]
       :from   [{:select [:tmp.* [(hsql/raw "ROW_NUMBER() OVER()") :rn]]
                 :from   [[(merge {:select [:*]}
                                  honeysql-query)
                           :tmp]]}]
       :where  [(hsql/raw (format "rn BETWEEN %d AND %d" offset (+ offset items)))]})))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql date workarounds                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Filtering with dates causes a -245 error. ;;v0.33.x
;; Explicit cast to timestamp when Date function is called to prevent db2 unknown parameter type.
;; Maybe it could not to be necessary with the use of DB2_DEFERRED_PREPARE_SEMANTICS
(defmethod sql.qp/->honeysql [:db2 Date]
  [_ date]
  		(hx/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date))) ;;v0.34.x needs it?
  ;;(hx/->timestamp (du/format-date "yyyy-MM-dd HH:mm:ss" date))) ;;v0.33.x

(defmethod sql.qp/->honeysql [:db2 Timestamp]
  [_ date]
  		(hx/->timestamp (t/format "yyyy-MM-dd HH:mm:ss" date)))

;; MEGA HACK from sqlite.clj ;;v0.34.x
;; Fix to Unrecognized JDBC type: 2014. ERRORCODE=-4228 
(defn- zero-time? [t]
  (= (t/local-time t) (t/local-time 0)))

(defmethod sql.qp/->honeysql [:db2 LocalDate]
  [_ t]
  (hsql/call :date (hx/literal (du/format-sql t))))

(defmethod sql.qp/->honeysql [:db2 LocalDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    (hsql/call :datetime (hx/literal (du/format-sql t)))))

(defmethod sql.qp/->honeysql [:db2 LocalTime]
  [_ t]
  (hsql/call :time (hx/literal (du/format-sql t))))

(defmethod sql.qp/->honeysql [:db2 OffsetDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    (hsql/call :datetime (hx/literal (du/format-sql t)))))

(defmethod sql.qp/->honeysql [:db2 OffsetTime]
  [_ t]
  (hsql/call :time (hx/literal (du/format-sql t))))

(defmethod sql.qp/->honeysql [:db2 ZonedDateTime]
  [driver t]
  (if (zero-time? t)
    (sql.qp/->honeysql driver (t/local-date t))
    (hsql/call :datetime (hx/literal (du/format-sql t)))))

;; (.getObject rs i LocalDate) doesn't seem to work, nor does `(.getDate)`; ;;v0.34.x
;; Merged from vertica.clj e sqlite.clj. 
;; Fix to Invalid data conversion: Wrong result column type for requested conversion. ERRORCODE=-4461
(defmethod sql-jdbc.execute/read-column [:db2 Types/DATE]
		[_ _ ^ResultSet rs _ ^Integer i]
		  (let [s (.getString rs i)
		        t (du/parse s)]
		    t))

(defmethod sql-jdbc.execute/read-column [:db2 Types/TIME]
		[_ _ ^ResultSet rs _ ^Integer i]
		  (let [s (.getString rs i)
		        t (du/parse s)]
		    t))

(defmethod sql-jdbc.execute/read-column [:db2 Types/TIMESTAMP]
		[_ _ ^ResultSet rs _ ^Integer i]
		  (let [s (.getString rs i)
		        t (du/parse s)]
		    t))

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
      (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))

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
    :XML          :type/Text
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
