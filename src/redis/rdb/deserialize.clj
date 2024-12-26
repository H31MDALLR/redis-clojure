(ns redis.rdb.deserialize
  (:require
   [clojure.java.io :as io]
   [clojure.walk :as walk]

   [clj-commons.byte-streams :as bs]
   [gloss.io :as gio :refer [decode-stream-headers lazy-decode-all]]
   [manifold.stream :as ms]
   [taoensso.timbre :as log]

   [redis.rdb.schema :as schema]
   [redis.rdb.transform :as transform]
   [redis.time :as time]))

; ----------------------------------------------------------------------------- Layer 0
; No deps on anything higher in the tree.

; --------------------------------------------------------- Util fns

(defn apply-f-to-key 
  [m k f]
  (walk/postwalk
   (fn [x]
     (if (and (map? x) (contains? x k))
       (update x k f)
       x))
   m))


(defn binary-array->string
  [arr]
  (String. (byte-array arr)  java.nio.charset.StandardCharsets/UTF_8))


(defn empty-db
  [id]
  {:aux          {}
   :id           id
   :resizdb-info {:db-hash-table-size     0
                  :expiry-hash-table-size 0}
   :database     {}})


(defn load-rdb-file
  [path]
  (-> path
      io/input-stream
      (bs/convert (bs/stream-of bytes))))

; --------------------------------------------------------- RDB parser streams
(defn body-stream [buffer]
  (gio/lazy-decode-all (schema/parse-section-selector) buffer))


(defn header-stream [db]
  (gio/decode-stream-headers db (schema/parse-rdb-header)))

; ---------------------------------------------------------  Parsing helpers
(defn deserialize 
  "Loop through a lazy stream and returns the results"
  [source]
  (loop [source source
         result []]
    (let [parsed (first source)]
      (log/trace ::deserialize {:parsed parsed
                                :result result})
      (if-not (seq parsed)
        result
        (recur (rest source) (conj result parsed))))))


(defn synchronous-take! 
  [stream]
  @(ms/take! stream))


(defn transform 
  [data]
  (transform/transform-data data))

; ----------------------------------------------------------------------------- Layer 1
; Only depends on layer 0

; --------------------------------------------------------- Load DB and parse

(defn parse-rdb-file [db-path]
  (let [db                        (load-rdb-file db-path)
        decoder-ring-magic-header (header-stream db)
        header                    (synchronous-take! decoder-ring-magic-header)
        buffer                    (synchronous-take! decoder-ring-magic-header)
        section-reader            (body-stream buffer)
        parsed-output             (deserialize section-reader)
        results                   (apply-f-to-key parsed-output :k binary-array->string)]
    (conj [header] results)))

; ----------------------------------------------------------------------------- Layer 2
; Only depends on layer 1
(defn rdb-file->database
  "Parse an RDB file and load it into memory as an EDN database"
  [db-path]
  (try
  ;; TBD handle paths that do not exist by creating an in memory empty db.
    (-> db-path
        parse-rdb-file
        second
        transform)
    (catch java.io.FileNotFoundException fnfe
      (log/warn ::rdb-file->database {:anomalies :anomalies/not-found
                                      :message   (ex-message fnfe)
                                      :cause     (ex-cause fnfe)})
      ;; return an empty database if no file is found
      (empty-db 0))))

; ----------------------------------------------------------------------------- REPL

(comment

  (-> "resources/test/rdb/dump.rdb"
      parse-rdb-file
      second
      transform/transform-data)

  (do (defn parse-rdb-file-ex 
        [db-path]
        (let [db                        (load-rdb-file db-path)
              decoder-ring-magic-header (header-stream db)
              header                    (synchronous-take! decoder-ring-magic-header)
              buffer                    (synchronous-take! decoder-ring-magic-header)
              section-reader            (body-stream buffer)
              parsed-output             (deserialize section-reader)]
          (conj [header] parsed-output)))
      

      (defn parse-rdb-file->edn
        "Parse RDB file and write the full untruncated result to an EDN file"
        [input-path output-path]
        (spit output-path 
              (binding [*print-length* nil  ; Prevent truncation
                        *print-level* nil]  ; Prevent nested structure truncation
                (pr-str (parse-rdb-file-ex input-path)))))
      
      (parse-rdb-file->edn
       "resources/test/rdb/dump.rdb"
       "resources/test/db/deserialized.edn"))

  (parse-rdb-file "resources/test/rdb/dump.rdb")

  (do
    (require '[java-time.api :as jt]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def sample-expiry-db [{:type :aux
                            :k    [114 101 100 105 115 45 118 101 114]
                            :v    [55 46 50 46 48]}
                           {:type :aux
                            :k    [114 101 100 105 115 45 98 105 116 115]
                            :v    [64]}
                           {:type      :selectdb
                            :db-number {:size 0}}
                           {:type                   :resizdb-info
                            :db-hash-table-size     {:size 3}
                            :expiry-hash-table-size {:size 3}}
                           {:type   :key-value
                            :expiry {:expiry 44172959069306880N
                                     :type   :expiry-ms
                                     :unit   :milliseconds}
                            :kind   :RDB_TYPE_STRING
                            :k      [109 97 110 103 111]
                            :v      [97 112 112 108 101]}
                           {:type   :key-value
                            :expiry {:expiry 3422276229857280N
                                     :type   :expiry-ms
                                     :unit   :milliseconds}
                            :kind   :RDB_TYPE_STRING
                            :k      [98 108 117 101 98 101 114 114 121]
                            :v      [112 101 97 114]}
                           {:type   :key-value
                            :expiry {:expiry 3422276229857280N
                                     :type   :expiry-ms
                                     :unit   :milliseconds}
                            :kind   :RDB_TYPE_STRING
                            :k      [112 101 97 114]
                            :v      [111 114 97 110 103 101]}])
    (-> sample-expiry-db
        transform/transform-data
        (utils/apply-f-to-key :k binary-array->string)
        ;time/expired?
        ))
  
  (do
    (require '[clojure.edn :as edn]
             '[java-time.api :as jt]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def sample-db (-> "test/db/deserialized.edn"
                       io/resource
                       slurp
                       edn/read-string))
    (-> sample-db
        (apply-f-to-key :k (comp keyword binary-array->string))
        transform/transform-data
        :database
        :strawberry
        :expiry
        time/expired?))

  (-> (jt/instant 44172959069306880N)
      time/expired?)

  ::leave-this-here
  )
