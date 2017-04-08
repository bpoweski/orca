(ns orca.core
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.data :refer [diff]]
            [clojure.set :as set]
            [clojure.core.match :refer [match]]
            [cheshire.core :as json])
  (:import [org.apache.hadoop.hive.ql.exec.vector
            VectorizedRowBatch ColumnVector DecimalColumnVector LongColumnVector BytesColumnVector TimestampColumnVector ListColumnVector]
           [org.apache.orc OrcFile Reader Writer TypeDescription TypeDescription$Category]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.hive.serde2.io HiveDecimalWritable]
           [java.nio.charset Charset]
           [java.time Duration Instant LocalDate]
           [java.time.temporal ChronoUnit]))


(def ^Charset serialization-charset (Charset/forName "UTF-8"))

(set! *warn-on-reflection* true)

(defn to-path
  [x]
  {:post (instance? Path %)}
  (cond
    (instance? java.net.URL x) (Path. (.toURI ^java.net.URL x))
    (instance? java.io.File x) (Path. (.getPath ^java.io.File x))
    (string? x)                (Path. ^String x)
    (instance? Path x)         x))

(defn ^Reader file-reader
  "Creates an ORC reader for a given file or path."
  [path]
  (OrcFile/createReader (to-path path) (OrcFile/readerOptions (Configuration.))))

(defprotocol ColumnValueReader
  (read-value [col schema idx]))

(defprotocol ColumnValueWriter
  (write-value [col idx v]))

(defprotocol ByteConversion
  (to-bytes [x]))

(defprotocol RowWriter
  (write-row! [row batch idx schema]))

(defprotocol LongConversion
  (to-long [ld]))

(defprotocol InstantConversion
  (to-instant [ld]))

(defn decode-column [^ColumnVector col schema nrows]
  (loop [idx 0
         result (transient [])]
    (if (< idx nrows)
      (if (or (.noNulls col) (not (aget (.isNull col) idx)))
        (recur (inc idx) (conj! result (read-value col schema idx)))
        (recur (inc idx) (conj! result nil)))
      (persistent! result))))

(defn read-batch [frame ^VectorizedRowBatch batch ^TypeDescription schema]
  (let [nrows (.size batch)]
    (loop [frame frame
           [[i ^ColumnVector col column-name column-type] & more] (map vector (range) (.cols batch) (map keyword (.getFieldNames schema)) (.getChildren schema))]
      (let [coll  (get frame column-name [])
            frame (assoc frame column-name (into coll (decode-column col column-type nrows)))]
        (if (seq more)
          (recur frame more)
          frame)))))

(defn read-vectors
  "Synchrounously reads column vectors from input."
  [input]
  (let [reader        (file-reader (to-path input))
        schema        (.getSchema reader)
        batch         (.createRowBatch schema)
        record-reader (.rows reader)]
    (loop [frame {}]
      (if (.nextBatch record-reader batch)
        (recur (read-batch frame batch schema))
        frame))))

(defprotocol TypeInference
  (data-type [v])
  (data-props [v]))

(derive ::array ::compound)
(derive ::map ::compound)
(derive ::struct ::compound)
(derive ::union ::compound)

(extend-protocol TypeInference
  (Class/forName "[C")
  (data-type [v] ::char)
  (data-props [v]))

(extend-protocol TypeInference
  ;; array      ListColumnVector
  java.util.List
  (data-type [v]
    (when (seq v)
      ::array))
  (data-props [v])

  ;; binary     BytesColumnVector

  ;; bigint     LongColumnVector
  java.math.BigInteger
  (data-type [v] ::bigint)
  (data-props [v])

  ;; boolean    LongColumnVector
  java.lang.Boolean
  (data-type [v] ::boolean)
  (data-props [v])

  ;; char       BytesColumnVector
  java.lang.Character
  (data-type [v] ::char)
  (data-props [v] {:length 1})

  ;; date       LongColumnVector
  java.time.LocalDate
  (data-type [v] ::date)
  (data-props [v])

  ;; org.joda.time.LocalDate
  ;; (data-type [v] ::date)
  ;; (data-props [v])

  ;; decimal    DecimalColumnVector
  java.math.BigDecimal
  (data-type [v] ::decimal)
  (data-props [v] {:scale (.scale v) :precision (.precision v)})

  ;; float      DoubleColumnVector
  java.lang.Float
  (data-type [v] ::float)
  (data-props [v])

  ;; double     DoubleColumnVector
  java.lang.Double
  (data-type [v] ::double)
  (data-props [v])

  ;; int        LongColumnVector
  ;; long       LongColumnVector
  ;; smallint   LongColumnVector
  ;; tinyint    LongColumnVector
  java.lang.Number
  (data-type [v]
    (let [x (long v)]
      (cond
        (>= x Byte/MIN_VALUE)    (cond
                                   (<= x Byte/MAX_VALUE)    ::tinyint
                                   (<= x Short/MAX_VALUE)   ::smallint
                                   (<= x Integer/MAX_VALUE) ::int
                                   :else ::bigint)
        (>= x Short/MIN_VALUE)   ::smallint
        (>= x Integer/MIN_VALUE) ::int
        :else                    ::bigint)))
  (data-props [v])

  ;; map        MapColumnVector
  java.util.Map
  (data-type [v] ::struct)
  (data-props [v])

  ;; struct     StructColumnVector

  ;; timestamp  TimestampColumnVector
  java.time.Instant
  (data-type [v] ::timestamp)
  (data-props [v])

  ;; org.joda.time.DateTime
  ;; (data-type [v] ::timestamp)
  ;; (data-props [v])

  ;; uniontype  UnionColumnVector

  ;; string     BytesColumnVector
  ;; varchar    BytesCoumnVector
  java.lang.String
  (data-type [v] ::string)
  (data-props [v])

  clojure.lang.Named
  (data-type [v] ::string)
  (data-props [v])

  nil
  (data-type [v])
  (data-props [v]))

(defn stats [coll]
  (let [nrows (count coll)
        coll  (remove nil? coll)]
    {:sum   (reduce + coll)
     :min   (apply min coll)
     :max   (apply max coll)
     :count nrows}))

(defn parse-file []
  (cheshire.core/parse-string (slurp (io/resource "search.json")) keyword))

(defmulti typedef data-type)

(defmethod typedef :default [x]
  (if-let [props (data-props x)]
    [(data-type x) props]
    [(data-type x)]))

(defmethod typedef ::map [x]
  [::map
   (reduce-kv
    (fn [kmap k v]
      (if-let [dt (data-type v)]
        (assoc kmap k (typedef v))
        kmap))
    {}
    x)])

(defmethod typedef ::struct [x]
  [::struct
   (reduce-kv
    (fn [kmap k v]
      (if-let [dt (data-type v)]
        (assoc kmap k (typedef v))
        kmap))
    {}
    x)])

(defmethod typedef ::array [x]
  (let [child-types (set (map typedef (remove nil? x)))
        n-types     (count child-types)
        tdef        [::array]]
    (cond
      (zero? n-types) tdef
      (= n-types 1)   (conj tdef (first child-types))
      :else           (conj tdef child-types))))

(defn type-description
  "Creates an ORC TypeDescription"
  [[dtype opts]]
  (case dtype
    ::boolean   (TypeDescription/createBoolean)
    ::tinyint   (TypeDescription/createByte)
    ::smallint  (TypeDescription/createShort)
    ::int       (TypeDescription/createInt)
    ::bigint    (TypeDescription/createLong)
    ::float     (TypeDescription/createFloat)
    ::double    (TypeDescription/createDouble)
    ::string    (TypeDescription/createString)
    ::date      (TypeDescription/createDate)
    ::timestamp (TypeDescription/createTimestamp)

    ;; ::binary
    ::decimal   (let [{:keys [scale precision]} opts]
                  (cond-> (TypeDescription/createDecimal)
                    (number? scale) (.withScale scale)
                    (number? precision) (.withPrecision precision)))
    ;; ::varchar
    ;; ::char
    ::array     (TypeDescription/createList (type-description opts))
    ::map       (let [key-types (set (map typedef (keys opts)))
                      ktype     (if (> (count key-types) 1)
                                  (type-description [::union key-types])
                                  (type-description (first key-types)))
                      val-types (set (vals opts))
                      vtype     (if (> (count val-types) 1)
                                  (type-description [::union val-types])
                                  (type-description (first val-types)))]
                  (TypeDescription/createMap ktype vtype))
    ::struct    (let [struct (TypeDescription/createStruct)]
                  (doseq [[k v] opts]
                    (.addField struct (name k) (type-description v)))
                  struct)
    ::union     (let [utype (TypeDescription/createUnion)]
                  (doseq [child opts]
                    (.addUnionChild utype (type-description child)))
                  utype)))

(defn infer-typedesc [x]
  (str (type-description (typedef x))))

(defn compound? [x]
  (isa? x ::compound))

(defn primitive? [x]
  (not (compound? x)))

(defn merge-schema
  ([x] x)
  ([x y]
   (if (= x y)
     x
     (match [x y]
       [[::union x-opts] [::union y-opts]]              (update x 1 set/union y-opts)
       [[::struct x-opts] [::struct y-opts]]            (update x 1 #(merge-with merge-schema %1 y-opts))
       [[::array x-opts] [::array y-opts]]              [::array (merge-schema x-opts y-opts)]
       [[::union x-opts] [_ :guard #(not= % ::union)]]  (update x 1 conj y)
       [[_ :guard #(not= % ::union)] [::union _]]       (update y 1 conj x)
       [[_ :guard primitive?] [_ :guard primitive?]]    [::union #{x y}]
       :else (pprint [x y])))))

(defn rows->schema [rows]
  (->> rows
       (map typedef)
       (reduce merge-schema)))

(extend-protocol ByteConversion
  java.lang.String
  (to-bytes [s] (.getBytes s serialization-charset)))

(extend-protocol InstantConversion
  Instant
  (to-instant [x] x)

  ;; org.joda.time.ReadableInstant
  ;; (to-instant [x] (Instant/ofEpochMilli (.getMillis x)))
  )

(extend-protocol LongConversion
  Number
  (to-long [x] (long x))

  LocalDate
  (to-long [x] (.toEpochDay x))

  ;; org.joda.time.LocalDate
  ;; (to-long [x] (to-long (LocalDate/of (.getYear x) (.getMonthOfYear x) (.getDayOfMonth x))))

  Boolean
  (to-long [b] (case b true 1 false 0)))

(extend-type DecimalColumnVector
  ColumnValueReader
  (read-value [arr schema idx]
    (let [^HiveDecimalWritable d (aget (.vector arr) idx)]
      (.bigDecimalValue (.getHiveDecimal d)))))

(extend-type LongColumnVector
  ColumnValueReader
  (read-value [arr ^TypeDescription schema idx]
    (condp = (.getCategory schema)
      TypeDescription$Category/DATE (LocalDate/ofEpochDay (aget (.vector arr) idx))
      (aget (.vector arr) idx)))

  ColumnValueWriter
  (write-value [col idx v]
    (aset-long (.vector col) idx (to-long v))))

(extend-type BytesColumnVector
  ColumnValueReader
  (read-value [arr schema idx]
    (String. ^"[B" (aget (.vector arr) idx) (aget (.start arr) idx) (aget (.length arr) idx) serialization-charset))

  ColumnValueWriter
  (write-value [col idx v]
    (.setVal col idx (to-bytes v))))

(extend-type TimestampColumnVector
  ColumnValueReader
  (read-value [arr schema idx]
    (.plusNanos (Instant/ofEpochMilli (aget (.time arr) idx)) (.getNanos arr idx)))

  ColumnValueWriter
  (write-value [col idx v]
    (.set col idx (java.sql.Timestamp/from ^Instant (to-instant v)))))

(extend-type ListColumnVector
  ColumnValueReader
  (read-value [col ^TypeDescription schema idx]
    (let [offset       (aget (.offsets col) idx)
          len          (aget (.lengths col) idx)
          child-col    (.child col)
          child-schema (first (.getChildren schema))]
      (mapv #(read-value child-col child-schema %) (range offset (+ offset len)))))

  ColumnValueWriter
  (write-value [col idx v]
    (let [child-col   (.child col)
          child-count (.childCount col)
          elems       (count v)
          _           (aset-long (.offsets col) idx child-count)
          _           (.ensureSize child-col (+ child-count elems) true)]
      (doseq [elem v
              :let [child-offset (.childCount col)]]
        (write-value (.child col) child-offset elem)
        (set! (.childCount col) (inc child-offset)))
      (aset-long (.lengths col) idx elems))))

(defn set-null! [^ColumnVector col ^long idx]
  (set! (.noNulls col) false)
  (aset-boolean (.isNull col) idx true))

(extend-protocol RowWriter
  clojure.lang.IPersistentMap
  (write-row! [row ^VectorizedRowBatch batch idx ^TypeDescription schema]
    (doseq [[^ColumnVector col field] (map vector (.cols batch) (.getFieldNames schema))]
      (let [val (get row (keyword field))]
        (if (nil? val)
          (set-null! col idx)
          (write-value col idx val)))))

  clojure.lang.Sequential
  (write-row! [row ^VectorizedRowBatch batch idx schema]
    (doseq [[^ColumnVector col v] (map vector (.cols batch) row)]
      (if (nil? v)
        (set-null! col idx)
        (write-value col idx v)))))

(defn write-rows
  "Write row-seq into an ORC file at path.

  Options:

  :overwrite?  - overwrites path if a file exists."
  [path row-seq schema & {:keys [overwrite?] :or {overwrite? false}}]
  (try
    (when overwrite?
      (.delete (io/file path)))
    (let [conf    (Configuration.)
          schema  (TypeDescription/fromString schema)
          options (.setSchema (OrcFile/writerOptions conf) schema)
          writer  (OrcFile/createWriter (to-path path) options)
          batch   (.createRowBatch schema)]
      (try
        (doseq [row-batch (partition-all 1024 row-seq)
                :let [batch-size (count row-batch)
                      _          (.ensureSize batch batch-size)]]
          (doseq [row row-batch
                  :let [idx (.size batch)]]
            (set! (.size batch) (inc idx))
            (write-row! row batch idx schema))
          (.addRowBatch writer batch)
          (.reset batch))
        (finally
          (.close writer))))
    (catch Exception ex
      (clojure.stacktrace/print-cause-trace ex)
      (throw ex))))

(defn tmp-path []
  (let [tmp (java.io.File/createTempFile "test" (str (rand-int (Integer/MAX_VALUE))))
        path (.getPath tmp)]
    (.delete tmp)
    path))

(defn frame->vecs [frame]
  (apply map vector (vals frame)))

(defn frame->maps [frame]
  (map zipmap (repeat (keys frame)) (frame->vecs frame)))
