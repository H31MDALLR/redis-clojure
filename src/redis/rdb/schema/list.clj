(ns redis.rdb.schema.list 
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]
   [taoensso.timbre :as log]))

(defn parse-list []
  (log/trace ::parse-list :enter)
  (gloss/ordered-map
   :encoding :list
   :items (gloss/repeated (string/parse-string-encoded-value) {:prefix (primitives/parse-length)})))

(defn parse-list-quicklist-2
  []
  (log/trace ::parse-list-quicklist-2 :enter)
  (gloss/ordered-map
   :encoding :quicklist-2
   :fake-node-count (primitives/parse-length-prefix)
   :item-count (primitives/parse-length-prefix)
   :listpack (string/parse-string-encoded-value)))