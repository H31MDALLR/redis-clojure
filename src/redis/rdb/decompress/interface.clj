(ns redis.rdb.decompress.interface 
  (:require
   [redis.rdb.decompress.core :as core]))

(defn inflate 
  "Decompress a value map's data, returning the uncompressed data"
  [{:keys [uncompressed-length] :as data}]
  (core/inflate data))

(comment 
  
  (ns-unalias *ns* 'core)
  ::leave-this-here)