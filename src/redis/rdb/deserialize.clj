(ns redis.rdb.deserialize
  (:require
   [clj-commons.byte-streams :as bs]
   [clojure.java.io :as io]
   [gloss.io :as gio]
   [manifold.stream :as ms]
   [redis.rdb.schema.core :as schema]
   [redis.rdb.schema.util :as util]
   [redis.rdb.transform :as transform]
   [redis.time :as time]
   [taoensso.timbre :as log]
   [manifold.stream :as s]))

; ----------------------------------------------------------------------------- Layer 0
; No deps on anything higher in the tree.

; --------------------------------------------------------- Util fns


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
        results                   (util/stringize-keys :k [:data] parsed-output)]
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

  (ns-unalias *ns* 'schema)
  (-> "resources/test/rdb/dump.rdb"
      parse-rdb-file
      second
      transform/transform-data)

  ;; ------------------------------------------------------ Deserialization debugging
  (do
    (log/set-level! :trace)

    (defn deserialize-some [db-path n]
      (let [db                        (load-rdb-file db-path)
            decoder-ring-magic-header (header-stream db)
            header                    (synchronous-take! decoder-ring-magic-header)
            buffer                    (synchronous-take! decoder-ring-magic-header)
            section-reader            (take n (body-stream buffer))
            parsed-output             (deserialize section-reader)]
        [db buffer (conj [header] (util/kewordize-keys parsed-output))]))

    (defn parse-rdb-file-ex
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
                      *print-level*  nil]  ; Prevent nested structure truncation
              (pr-str (parse-rdb-file input-path))))))

  (parse-rdb-file->edn
   "resources/test/rdb/dump.rdb"
   "resources/test/db/deserialized.edn")
  
  (parse-rdb-file "resources/test/rdb/dump.rdb")
  
  (deserialize-some "resources/test/rdb/dump.rdb" 21)

  (util/bytes->string (byte-array [100 111 117 98 108 101 118 97 108 117 101]))

  ;; ------------------------------------------------------ Debug key expiry
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
        (utils/apply-f-to-key :k (comp keyword utils/binary-array->string))
        transform/transform-data
        :database
        :strawberry
        :expiry
        time/expired?))
  
  (-> (jt/instant 44172959069306880N)
      time/expired?)

  ::leave-this-here
  )
