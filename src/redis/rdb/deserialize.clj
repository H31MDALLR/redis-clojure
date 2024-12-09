(ns redis.rdb.deserialize
  (:require
   [clojure.java.io :as io]
   [clojure.walk :as walk]

   [clj-commons.byte-streams :as bs]
   [gloss.io :as gio :refer [decode-stream-headers lazy-decode-all]]
   [manifold.stream :as ms]
   [taoensso.timbre :as log]

   [redis.rdb.schema :as schema]
   [redis.rdb.transform :as transform]))

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
      io/resource
      io/input-stream
      (bs/convert (bs/stream-of bytes))))

; --------------------------------------------------------- RDB parser streams
(defn body-stream [buffer]
  (gio/lazy-decode-all schema/section-selector buffer))


(defn header-stream [db]
  (gio/decode-stream-headers db schema/rdb-header))

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
  (-> "test/rdb/dump.rdb"
      parse-rdb-file
      second
      transform/transform-data)
  (rdb-file->database "test/rdb/dump.rdb"))