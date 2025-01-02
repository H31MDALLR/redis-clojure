(ns redis.rdb.schema.string
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.util :as util]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Layer 0
;; Depends only on things outside of this namespace

(defn encode-string-header
  "Encode the string header"
  [{:keys [kind size special data]
    :as   m}]
  (log/trace ::parse-string  {:kind    kind
                              :size    size
                              :special special
                              :m       m})
  {:kind    (or kind 0)
   :size    (if (number? size)
              size
              (count (or data [])))
   :special special})


(defn parse-string
  [kind special size]
  (if (= kind :RDB_TYPE_STRING)
    (gloss/ordered-map
     :encoding :string
     :kind kind
     :special special
     :size size
     :data  (-> size
                (primitives/repeat-parser :byte)
                util/bytes->string))
    (gloss/ordered-map
     :encoding :any
     :kind kind
     :special special
     :size size
     :data (primitives/repeat-parser size :byte))))

;; ------------------------------------------------------------------------------------------- Layer 1
;; Depends only on Layer 0


(defn parse-lzf-string []
  (gloss/header
   (primitives/parse-length)
   (fn [{:keys [kind size special]}]
     (log/trace ::parse-lzf-string :compressed-length size)
     (gloss/compile-frame (gloss/ordered-map
                           :encoding :lzh-string
                           :kind kind
                           :size size
                           :special special
                           :uncompressed-length (primitives/parse-length)
                           :data (primitives/repeat-parser size :byte))))
   encode-string-header))

;; ------------------------------------------------------------------------------------------- Layer 2
;; Depends only on Layer 1

(defn parse-string-encoded-value
  []
  (gloss/compile-frame
   (gloss/header
    (primitives/parse-length)
    (fn [{:keys [kind size special]
          :as   header}]
      (log/trace ::parse-string header)
      (if special
        ;; Special encoding
        (condp = special
          0 (gloss/compile-frame (gloss/ordered-map :type :int-string
                                                    :kind kind
                                                    :special special
                                                    :data [:byte]))
          1 (gloss/compile-frame (gloss/ordered-map :type :int-string
                                                    :kind kind
                                                    :special special
                                                    :data [:int16-le]))
          2 (gloss/compile-frame (gloss/ordered-map :type :int-string
                                                    :kind kind
                                                    :special special
                                                    :data [:int32-le]))
          3 (parse-lzf-string)
          (throw (Exception. (str "Unknown special encoding: " special))))
        ;; Regular string
        (parse-string kind special size)))
    encode-string-header)))
