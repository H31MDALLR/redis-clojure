(ns redis.rdb.decoding.stream
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]
            [redis.rdb.decoding.string :as string]))

(defn- decode-stream-id [ms seq]
  (str ms "-" seq))

(defn- decode-stream-entry [entry]
  (let [{:keys [id fields]} entry]
    {:id id
     :fields (into {} (partition 2 fields))}))

(defn- decode-listpack-stream [data]
  (let [{:keys [header entries metadata]} data
        {:keys [flags encoding]} metadata]
    {:header header
     :entries (mapv decode-stream-entry entries)
     :metadata {:flags flags
                :encoding encoding
                :uncompressed-length (:uncompressed-length data)}}))

(defmethod decode-type :stream [{:keys [type data]} _]
  (case type
    :string (mapv (fn [[id timestamp value]]
                   {:id (string/decode-string id)
                    :timestamp (Long/parseLong (string/decode-string timestamp))
                    :value (string/decode-string value)})
                 (partition 3 data))
    :listpack (decode-listpack-stream data)
    :listpack-collection (mapv decode-listpack-stream data)
    (throw (ex-info "Unsupported storage type for stream"
                   {:storage-type type}))))

(defn stream->value [data]
  {:entries (mapv :fields (:entries data))
   :metadata (:metadata data)})
