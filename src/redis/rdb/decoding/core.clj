(ns redis.rdb.decoding.core)

(defn rdb-type->encoding-tuple [t]
  (let [encoding-map {:RDB_TYPE_HASH                    [::string :hash]
                      :RDB_TYPE_HASH_LISTPACK           [::listpack :hash]
                      :RDB_TYPE_HASH_LISTPACK_EX        [::listpack-ex :hash]
                      :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA [::listpack-ex-pre-ga :hash]
                      :RDB_TYPE_HASH_METADATA           [::metadata :hash]
                      :RDB_TYPE_HASH_METADATA_PRE_GA    [::metadata-pre-ga :hash]
                      :RDB_TYPE_HASH_ZIPLIST            [::ziplist :hash]
                      :RDB_TYPE_HASH_ZIPMAP             [::zipmap :hash]
                      :RDB_TYPE_LIST                    [::listpack :list]
                      :RDB_TYPE_LIST_QUICKLIST          [::quicklist :list]
                      :RDB_TYPE_LIST_QUICKLIST_2        [::quicklist-v2 :list]
                      :RDB_TYPE_LIST_ZIPLIST            [::ziplist :list]
                      :RDB_TYPE_SET                     [::string :set]
                      :RDB_TYPE_SET_INTSET              [::intset :set]
                      :RDB_TYPE_SET_LISTPACK            [::listpack :set]
                      :RDB_TYPE_STREAM_LISTPACKS        [::listpack-collection :stream]
                      :RDB_TYPE_STREAM_LISTPACKS_2      [::listpack-v2-collection :stream]
                      :RDB_TYPE_STREAM_LISTPACKS_3      [::listpack-v3-collection :stream]
                      :RDB_TYPE_STRING                  [::bytes :string]
                      :RDB_TYPE_ZSET                    [::ziplist :zset]
                      :RDB_TYPE_ZSET_2                  [::ziplist :zset2]
                      :RDB_TYPE_ZSET_LISTPACK           [::ziplist :zset-listpack]
                      :RDB_TYPE_ZSET_ZIPLIST            [::ziplist :zset]}]
    (get encoding-map t)))

;; Storage format decoder
(defmulti decode-storage (fn [_ encoding] encoding))

;; Final type conversion
(defmulti decode-type (fn [_ type] type))


(defn decode-rdb-value 
  "Decode an RDB value into a Clojure data structure."
  [type value]
  (let [[storage-format dest-type] (rdb-type->encoding-tuple type)]
    (-> value
        (decode-storage storage-format)
        (decode-type dest-type))))
