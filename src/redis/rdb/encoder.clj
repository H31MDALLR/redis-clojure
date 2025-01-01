(ns redis.rdb.encoder
  (:require
   [java-time.api :as jt]
   [redis.rdb.schema.util :as util]
   [taoensso.timbre :as log]))

`
;; ----------------------------------------------------------------------------- Defs
(defn value->encoding [value]
  (let [encoding-map {:RDB_OPCODE_AUX                   bytes->string
                      :RDB_OPCODE_EXPIRETIME            #(comp bytes->string jt/instant)
                      :RDB_OPCODE_EXPIRETIME_MS         #(comp bytes->string jt/instant)
                      :RDB_OPCODE_RESIZEDB              identity
                      :RDB_OPCODE_SELECTDB              num
                      :RDB_TYPE_HASH                    [:hash :raw]
                      :RDB_TYPE_HASH_LISTPACK           [:hash :listpack]
                      :RDB_TYPE_HASH_LISTPACK_EX        [:hash :listpack-ex]
                      :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA [:hash :listpack-ex-pre-ga]
                      :RDB_TYPE_HASH_METADATA           [:hash :metadata]
                      :RDB_TYPE_HASH_METADATA_PRE_GA    [:hash :metadata-pre-ga]
                      :RDB_TYPE_HASH_ZIPLIST            [:hash :ziplist]
                      :RDB_TYPE_HASH_ZIPMAP             [:hash :zipmap]
                      :RDB_TYPE_LIST                    [:list :raw]
                      :RDB_TYPE_LIST_QUICKLIST          [:list :quicklist]
                      :RDB_TYPE_LIST_QUICKLIST_2        [:list :quicklist-v2]
                      :RDB_TYPE_LIST_ZIPLIST            [:list :ziplist]
                      :RDB_TYPE_MODULE_2                [:module :v2]
                      :RDB_TYPE_MODULE_PRE_GA           [:module :pre-ga]
                      :RDB_TYPE_SET                     [:set :raw]
                      :RDB_TYPE_SET_INTSET              [:set :intset]
                      :RDB_TYPE_SET_LISTPACK            [:set :listpack]
                      :RDB_TYPE_STREAM_LISTPACKS        [:stream :listpack]
                      :RDB_TYPE_STREAM_LISTPACKS_2      [:stream :listpack-v2]
                      :RDB_TYPE_STREAM_LISTPACKS_3      [:stream :listpack-v3]
                      :RDB_TYPE_STRING                  [:string :raw]
                      :RDB_TYPE_ZSET                    [:zset :string]
                      :RDB_TYPE_ZSET_2                  [:zset2 :string]
                      :RDB_TYPE_ZSET_LISTPACK           [:zset :listpack]
                      :RDB_TYPE_ZSET_ZIPLIST            [:zset :ziplist]}]))

; ----------------------------------------------------------------------------- Layer 0
;; no deps inside this file
(defn bytes->string
  "Convert a sequence of bytes (integers) into a UTF-8 string."
  [byte-seq]
  (log/trace ::bytes->string {:byte-seq byte-seq})
  (when byte-seq
    (cond
      (map? byte-seq) (util/bytes->string (:data byte-seq))
      (sequential? byte-seq) (String. (byte-array byte-seq) "UTF-8")
      :else (str byte-seq))))

; ----------------------------------------------------------------------------- Layer 1
;; only deps on layer 0

