(ns redis.rdb.schema.kv
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.hash :as hash]
   [redis.rdb.schema.list :as list]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.set :as set]
   [redis.rdb.schema.streams :as streams]
   [redis.rdb.schema.string :as string]
   [redis.rdb.schema.zset :as zset]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Layer 0
;; Depends only on things outside of this namespace

(defn value-kind->value
  [kind]
  (println ::value-kind->value kind)
  (gloss/compile-frame
   (case kind
     :RDB_TYPE_STRING                  (string/parse-string-encoded-value)
     :RDB_TYPE_LIST                    (list/parse-list)
     :RDB_TYPE_SET                     (set/parse-set)
     :RDB_TYPE_ZSET                    (zset/parse-zset)
     :RDB_TYPE_HASH                    (hash/parse-hash)
     :RDB_TYPE_ZSET_2                  (zset/parse-zset2) ;; ZSET version 2 with doubles stored in binary. */
     :RDB_TYPE_HASH_ZIPMAP             (string/parse-string-encoded-value)
     :RDB_TYPE_LIST_ZIPLIST            (string/parse-string-encoded-value)
     :RDB_TYPE_SET_INTSET              (string/parse-string-encoded-value)
     :RDB_TYPE_ZSET_ZIPLIST            (string/parse-string-encoded-value)
     :RDB_TYPE_HASH_ZIPLIST            (string/parse-string-encoded-value)
     :RDB_TYPE_LIST_QUICKLIST          (string/parse-string-encoded-value)
     :RDB_TYPE_STREAM_LISTPACKS        (streams/parse-stream-listpack-3-bypass)
     :RDB_TYPE_HASH_LISTPACK           (string/parse-string-encoded-value)
     :RDB_TYPE_HASH_LISTPACK_EX_PRE_GA (string/parse-string-encoded-value)
     :RDB_TYPE_ZSET_LISTPACK           (string/parse-string-encoded-value)
     :RDB_TYPE_LIST_QUICKLIST_2        (list/parse-list-quicklist-2)
     :RDB_TYPE_STREAM_LISTPACKS_2      (streams/parse-stream-listpack-3-bypass)
     :RDB_TYPE_SET_LISTPACK            (string/parse-string-encoded-value)
     :RDB_TYPE_STREAM_LISTPACKS_3      (streams/parse-stream-listpack-3-bypass)
     (throw (ex-info "Unknown kind" {:kind kind})))))

(defn parse-key-value
  ([kind] (parse-key-value {} kind))
  ([expiry-kind kind]
   (log/trace ::key-value {:kind kind})
   (gloss/compile-frame
    (gloss/ordered-map
     :type :key-value
     :expiry expiry-kind
     :kind kind
     :k (string/parse-string-encoded-value)
     :v (value-kind->value kind))
    ;; Add encoding function
    (fn [data]
      (log/trace ::key-value-encoding {:data data})
      (-> data
          (assoc :type :key-value)
          (update :kind #(or % kind))
          (update :expiry #(or % expiry-kind))))
    ;; For encoding, preserve the structure but ensure kind is passed through
    identity)))

;; ------------------------------------------------------------------------------------------- Layer 2
;; Depends only on Layer 1


(defn parse-key-value-with-expiry
  [expiry-kind]
  (gloss/compile-frame
   (gloss/header
    (primitives/parse-expiry expiry-kind)
    (fn [expiry-kind]
      (gloss/header
       (primitives/parse-header-byte)
       (fn [kind]
         (parse-key-value expiry-kind kind))
       identity))
   identity)))
