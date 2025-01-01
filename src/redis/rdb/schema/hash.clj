(ns redis.rdb.schema.hash 
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]
   [taoensso.timbre :as log]))


(defn parse-hash []
  (log/trace ::parse-hash :enter)
  (gloss/ordered-map
   :type :hash
   :entries (gloss/repeated
             (gloss/ordered-map
              :k (string/parse-string-encoded-value)
              :v (string/parse-string-encoded-value))
             {:prefix (gloss/prefix
                       (primitives/parse-length)
                       :size
                       primitives/encode-length)})))


