(ns redis.rdb.schema.zset
  (:require
   [taoensso.timbre :as log]
   [gloss.core :as gloss]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]))


(defn parse-scored-value
  []
  (log/trace ::parse-scored-value-encoding :enter)
  (gloss/compile-frame
   (gloss/header
    :byte
    (fn [header]
      (log/trace ::scored-value-encoding {:header header})
      (condp = header
        0xFD (primitives/value-parser :nan)
        0xFE (primitives/value-parser :pos-infinity)
        0xFF (primitives/value-parser :neg-infinity)
        ;; For regular values, header is the length of the string
        (gloss/compile-frame (gloss/ordered-map
                              :type :float
                              :value (gloss/string-float :ascii :length header)))))
    identity)))

(defn parse-zset []
  (log/trace ::parse-zset :enter)
  (gloss/header
   (primitives/parse-length)
   (fn [{:keys [size]}]
     (log/trace ::parse-zset {:elements size})
     (gloss/ordered-map
      :type :zset
      :entries (primitives/repeat-parser
                size
                (gloss/ordered-map
                 :v (string/parse-string-encoded-value)
                 :score (parse-scored-value)))))
   identity))


(defn parse-zset2 []
  (gloss/ordered-map
   :type :zset
   :entries (gloss/repeated
             (gloss/ordered-map
              :member (string/parse-string-encoded-value)
              :score (parse-scored-value))
             {:prefix (primitives/parse-length)})))
