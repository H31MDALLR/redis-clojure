(ns redis.rdb.schema.set 
  (:require
   [gloss.core :as gloss]
   [taoensso.timbre :as log]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]))


(defn parse-set []
  (log/trace ::parse-set :enter)
  (gloss/header
   (primitives/parse-length)
   (fn [{:keys [kind size]}]
     (log/trace ::parse-set {:elements size})
     (gloss/ordered-map
      :kind kind
      :encoding :list
      :items (repeat size (string/parse-string-encoded-value))))
   (fn [m]
     (log/trace ::parse-set {:m m})
     (let [{:keys [kind items]} m]
       {:kind kind
        :size (count items)}))))

