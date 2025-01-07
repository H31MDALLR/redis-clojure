(ns redis.rdb.decoding.listpack
  (:require
   [clojure.java.io :as io]
   [manifold.stream :as s]
   [redis.rdb.decoding.core :refer [decode-storage]]
   [redis.rdb.schema.bytes :as bytes]
   [redis.rdb.schema.listpack :as listpack]
   [redis.rdb.decompress.interface :as decompress]
   [taoensso.timbre :as log]))


;; Excellent treatise on listpacks
;; https://github.com/zpoint/Redis-Internals/blob/5.0/Object/listpack/listpack.md



(defn decode-stream-entries 
  "Decodes the entries of a stream"
  [{:keys [data uncompressed-length] :as v}]
  (log/trace ::decode-stream-entries {:data data 
                                      :uncompressed-len uncompressed-length})
  (try 
    (let [listpack (decompress/inflate v)
          stream (bytes/get-byte-stream-parser listpack (listpack/parse-listpack))]
      @(s/take! stream))
    (catch Exception e
      (log/error ::decode-stream-entries {:error e})
      (throw e))))

;; -------------------------------------------------------- Storage Format Decoders

(defmethod decode-storage :listpack [value _]
  {:type :listpack
   :data (decode-stream-entries value)})

(defmethod decode-storage :listpack-v2 [value _]
  (let [entries (decode-stream-entries value)]
    {:type :listpack
     :data (assoc entries :version 2)}))

(defmethod decode-storage :listpack-v3 [value _]
  (let [entries (decode-stream-entries value)
        flags (bit-and (first (:data value)) 0xFF)]
    {:type :listpack
     :data (assoc entries
                  :version 3
                  :metadata {:flags flags
                             :encoding (:type value)
                             :uncompressed-length (:uncompressed-length value)})}))

(defmethod decode-storage :listpack-collection [value _]
  {:type :listpack-collection
   :data (mapv decode-stream-entries value)})

(defmethod decode-storage :listpack-v2-collection [value _]
  {:type :listpack-collection
   :data (mapv #(assoc (decode-stream-entries %) :version 2) value)})

(defmethod decode-storage :listpack-v3-collection [value _]
  {:type :listpack-collection
   :data (mapv #(assoc (decode-stream-entries %)
                       :version 3
                       :metadata {:flags (bit-and (first (:data %)) 0xFF)
                                  :encoding (:type %)
                                  :uncompressed-length (:uncompressed-length %)})
               value)})

;; -------------------------------------------------------------------------- REPL

(comment
  
;; The structure of the listpack is:
;; <tot-bytes> <num-elements> <element-1> ... <element-N> <listpack-end-byte>
;; Where each element is of the structure:
;; <encoding-type><element-data><element-tot-len>.
;; Reference: https://github.com/antirez/listpack/blob/master/listpack.md


  
  (do
    (require '[clojure.edn :as edn]
             '[clojure.java.io :as io]
             '[java-time.api :as jt]
             '[redis.time :as time]
             '[redis.utils :as utils])
    (def input-data (-> "test/db/deserialized.edn"
                        io/resource
                        slurp
                        edn/read-string)))
  (get (second input-data) 9)

  (decode-storage (-> (second input-data) (get 9) :v) ::listpack)

  ::leave-this-here)