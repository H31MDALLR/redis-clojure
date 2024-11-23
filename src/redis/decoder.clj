(ns redis.decoder 
  (:require
   [clojure.string :as str]))

(defn keywordize [s] (-> s str/lower-case keyword))

(defmulti decode (fn [coll] (first coll)))
(defmethod decode "ERROR"
  [[command exception]]
  {:command (keywordize command)
   :exception exception})

(defmethod decode "PING"
  [[command msg]]
  {:command (keywordize command)
   :msg msg})

(defmethod decode "SET"
  [[command k v]]
  {:command (keywordize command)
   :k k
   :v v})


(comment 
  
  "Leave this here.")