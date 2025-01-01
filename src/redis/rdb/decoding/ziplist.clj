(ns redis.rdb.decoding.ziplist
  (:require [redis.rdb.decoding.core :refer [decode-storage decode-type]]))

;; -------------------------------------------------------------------------- defs

(def ziplist-header-size 10)
(def ziplist-entry-size 10)

;; -------------------------------------------------------------------------- decode impl

(defn- decode-ziplist-header [data]
  (let [header (subvec data 0 ziplist-header-size)]
    header))

(defn- decode-entries [data]
  (let [entries (subvec data ziplist-header-size)]
    entries))

;; -------------------------------------------------------- Storage Format Decoders

(defmethod decode-storage ::ziplist [value _]
  {:type :ziplist
   :data {:entries (decode-entries value)}}) 