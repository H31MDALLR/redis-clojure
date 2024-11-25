(ns redis.decoder 
  (:require
   [redis.parsing.options :as options]
   [redis.utils :refer [keywordize]]))

;; ------------------------------------------------------------------------------------------- Layer 0


;; ------------------------------------------------------------------------------------------- Layer 1
;; -------------------------------------------------------- Decode Interface

(defmulti decode (fn [coll] (first coll)))
(defmethod decode "COMMAND"
  [[command & args]]
  {:command (keywordize command)
   :args args})

(defmethod decode "ECHO"
  [parse-result]
  (options/parse-result->command parse-result 1))

(defmethod decode "ERROR"
  [[command exception]]
  {:command (keywordize command)
   :exception exception})

(defmethod decode "GET"
  [[command k]]
  {:command (keywordize command)
   :k (keywordize k)})

(defmethod decode "PING"
  [parse-result]
 (options/parse-result->command parse-result 1))

(defmethod decode "SET"
   [parse-result]
  (options/parse-result->command parse-result 2))

;; ------------------------------------------------------------------------------------------- REPL Area

(comment 
  
  "Leave this here.")