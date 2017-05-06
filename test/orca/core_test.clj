(ns orca.core-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [orca.core :as orc :refer :all])
  (:import [org.apache.hadoop.fs Path]
           [java.time Instant LocalDate]))


(deftest path-test
  (is (= (Path. (.toURI (io/resource "decimal.orc"))) (to-path (io/resource "decimal.orc")))))

(deftest orc-reader-test
  (let [[stats & _] (map stats (vals (read-vectors (io/resource "decimal.orc"))))]
    (is (= 6000 (:count stats)))
    (is (= 1999.2M (:max stats)))
    (is (= -1000.5M (:min stats)))
    (is (= 1998301.099M (:sum stats)))))

(deftest type-inference-test
  (testing "BigDecimal"
    (is (= ::orc/decimal (data-type 10.0M)))
    (is (= {:scale 1 :precision 3} (data-props 10.0M))))
  (testing "Boolean"
    (is (= ::orc/boolean (data-type true)))
    (is (= ::orc/boolean (data-type false))))
  (testing "Integer"
    (is (= ::orc/tinyint (data-type 10)))
    (is (= ::orc/tinyint (data-type 0)))
    (is (= ::orc/smallint (data-type Short/MAX_VALUE)))
    (is (= ::orc/smallint (data-type Short/MIN_VALUE)))
    (is (= ::orc/int (data-type (inc Short/MAX_VALUE))))
    (is (= ::orc/int (data-type (dec Short/MIN_VALUE))))
    (is (= ::orc/bigint (data-type (inc Integer/MAX_VALUE))))
    (is (= ::orc/bigint (data-type (dec Integer/MIN_VALUE)))))
  (testing "Float"
    (is (= ::orc/float (data-type (float -1.0))))
    (is (= ::orc/float (data-type Float/MAX_VALUE)))
    (is (= ::orc/float (data-type Float/MIN_VALUE))))
  (testing "Double"
    (is (= ::orc/double (data-type -1.0)))
    (is (= ::orc/double (data-type 100.00))))
  (testing "String"
    (is (= ::orc/string (data-type "")))
    (is (= ::orc/string (data-type "foo"))))
  (testing "Char"
    (is (= ::orc/char (data-type \newline)))
    (is (= ::orc/char (data-type (char-array [\f \o \o])))))
  (testing "DateTime"
    (is (= ::orc/timestamp (data-type (Instant/parse "2017-04-07T17:24:03.222Z")))))
  (testing "Date"
    (is (= ::orc/date (data-type (LocalDate/of 2017 4 3))))))

(deftest typedef-test
  (testing "Strings"
    (is (= ::orc/string (typedef "foo")))
    (is (= ::orc/string (typedef "10")))
    (is (= [::orc/decimal {:scale 0 :precision 2}] (typedef "10" {:coerce-decimal-strings? true})))
    (is (= [::orc/decimal {:scale 0 :precision 2}] (typedef "10" {:coerce-decimal-strings? true})))
    (is (= ::orc/date (typedef "2017-04-10" {:coerce-date-strings? true})))
    (is (= ::orc/timestamp (typedef "2017-05-07T06:39:18Z" {:coerce-timestamp-strings? true}))))
  (testing "Decimal"
    (is (= [::orc/decimal {:scale 2 :precision 10}] (typedef 10M {:min-decimal-scale 2 :min-decimal-precision 10}))))
  (testing "Arrays"
    (is (= [::orc/array ::orc/tinyint] (typedef [1])))
    (is (= [::orc/array ::orc/tinyint] (typedef [1 -1])))
    (is (= [::orc/array ::orc/tinyint] (typedef [1 nil]))))
  (testing "Arrays of compound types"
    (is (= [::orc/array
            #{[::orc/struct {:a ::orc/tinyint}]
              [::orc/struct
               {:a ::orc/smallint,
                :b ::orc/string}]
              [::orc/struct {:a ::orc/smallint}]}]
           (typedef [{:a 1} {:a 10000} {:a 10001 :b "foo"}]))))
  (testing "Map"
    (is (= [::orc/struct
            {:a    ::orc/tinyint
             "foo" ::orc/string
             10    ::orc/tinyint}]
           (typedef {:a 1 "foo" "bar" 10 11})))))

(defn infer-typedesc [x]
  (str (typedef->schema (typedef x))))

(deftest typedef->schema-test
  (testing "numeric"
    (is (= "tinyint" (infer-typedesc 1)))
    (is (= "smallint" (infer-typedesc 128)))
    (is (= "int" (infer-typedesc (inc Short/MAX_VALUE))))
    (is (= "bigint" (infer-typedesc (inc Integer/MAX_VALUE))))
    (is (= "float" (infer-typedesc (float 1))))
    (is (= "double" (infer-typedesc 1.0))))
  (testing "strings"
    (is (= "string" (infer-typedesc "hello"))))
  (testing "decimals"
    (is (= "decimal(2,1)" (infer-typedesc 1.0M))))
  (testing "struct"
    (is (= "struct<k:string,y:boolean>" (infer-typedesc {:k "foo" :y true}))))
  (testing "date"
    (is (= "date" (infer-typedesc (LocalDate/of 2017 1 1)))))
  (testing "timestamp"
    (is (= "timestamp" (infer-typedesc (Instant/now))))))

(deftest merge-typedef-test
  (testing "structs"
    (is (= [::orc/struct {:x [::orc/tinyint] :y [::orc/boolean]}] (merge-typedef [::orc/struct {:x [::orc/tinyint]}] [::orc/struct {:y [::orc/boolean]}]))))
  (testing "primitive integers"
    (is (= ::orc/smallint (merge-typedef ::orc/smallint ::orc/tinyint)))
    (is (= ::orc/bigint (merge-typedef ::orc/smallint ::orc/bigint)))
    (is (= ::orc/smallint (merge-typedef ::orc/tinyint ::orc/smallint)))
    (is (= ::orc/smallint (merge-typedef ::orc/smallint ::orc/smallint)))
    (is (= [::orc/smallint] (merge-typedef [::orc/smallint] [::orc/smallint]))))
  (testing "coercible"
    (is (= ::orc/double (merge-typedef ::orc/double ::orc/tinyint))))
  (testing "single values"
    (is (= ::orc/smallint (merge-typedef ::orc/smallint))))
  (testing "string encoded types"
    (is (= ::orc/string (merge-typedef [::orc/decimal {:scale 4 :precision 0}] ::orc/string))))
  (testing "decimals"
    (is (= [::orc/decimal {:scale 2, :precision 6}] (merge-typedef [::orc/decimal {:scale 2, :precision 6}] [::orc/decimal {:scale 1, :precision 6}]))))
  (testing "arrays"
    (is (= [::orc/array ::orc/int] (merge-typedef [::orc/array ::orc/tinyint] [::orc/array ::orc/int])))))

(deftest simplify-typedef-test
  (testing "arrays"
    (is (= [::orc/array ::orc/int] (simplify-typedef [::orc/array #{::orc/tinyint ::orc/int}]))))
  (testing "struct with array"
    (is (= [::orc/struct {:values [::orc/array ::orc/int]}] (simplify-typedef [::orc/struct {:values [::orc/array #{::orc/tinyint ::orc/int}]}]))))
  (testing "empty structs are pruned"
    (is (= [::orc/struct {:a ::orc/tinyint}] (simplify-typedef [::orc/struct {:values [::orc/struct {}] :a ::orc/tinyint}])))
    (is (nil? (simplify-typedef [::orc/struct {:values [::orc/struct {}]}])))))

(defn roundtrip [input schema]
  (let [tmp    (tmp-path)
        _      (write-rows tmp input schema)]
    (try
      (read-vectors tmp)
      (finally
        (.delete (io/file tmp))))))

(deftest round-trip-test
  (testing "vectors"
    (are [schema in] (= in (frame->vecs (roundtrip in schema)))
      "struct<x:int>"                    [[1] [2] [3]]
      "struct<x:int,y:string>"           [[1 "a"] [2 "b"]]
      "struct<y:date>"                   [[(LocalDate/of 2017 4 7)] [nil]]
      "struct<y:timestamp>"              [[(Instant/parse "2017-04-07T17:13:19.581Z")] [nil]]
      "struct<y:array<string>>"          [['()] [nil]]
      "struct<y:array<int>>"             [['()] ['(1 2 3)]]
      "struct<y:array<array<int>>>"      [['((1 2 3))]]
      "struct<y:struct<x:int>>"          [[{:x 1}]]
      "struct<y:struct<x:int,y:string>>" [[{:x 1}]]
      "struct<x:double>"                 [[1.00]]))
  (testing "frames"
    (are [schema in frame] (= frame (roundtrip in schema))
      "struct<x:int,y:string>"      [[nil "a"] [2 nil]]                                {:x [nil 2] :y ["a" nil]}
      "struct<x:string,y:int>"      [{:x "foo" :y 10} {:x "bar" :y 100000} {:z false}] {:x ["foo" "bar" nil] :y [10 100000 nil]}))
  (testing "type coersion"
    (are [schema in out] (= out (frame->vecs (roundtrip in schema)))
      "struct<x:timestamp>"        [["2017-04-07T17:13:19.581Z"]]  [[(Instant/parse "2017-04-07T17:13:19.581Z")]])))

(deftest to-long-test
  (testing "date"
    (is (= 17168 (to-long (LocalDate/of 2017 1 2)))))
  (testing "boolean"
    (is (= 1 (to-long true)))
    (is (= 0 (to-long false)))
    (is (= 1 (to-long 1)))))

(deftest to-instant-test
  (testing "strings"
    (is (= (Instant/parse "2017-04-07T17:13:19.581Z") (to-instant "2017-04-07T17:13:19.581Z" {})))))
