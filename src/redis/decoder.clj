(ns redis.decoder 
  (:require
   [redis.parsing.options :as options]
   [redis.utils :refer [keywordize]]))

;; ------------------------------------------------------------------------------------------- Layer 0


;; ------------------------------------------------------------------------------------------- Layer 1
;; -------------------------------------------------------- Decode Interface

(defmulti decode (fn [coll] (-> coll first keywordize)))
(defmethod decode :command
  [[command & args]]
  {:command (keywordize command)
   :args args})

;; special
(defmethod decode :config
  [[command & args]]
  (let [[subcommand & options] args]
    {:command (keywordize command)
     :subcommand subcommand
     :options options}))

(defmethod decode :echo
  [parse-result]
  (options/parse-result->command parse-result 1))

(defmethod decode :error
  [[command exception]]
  {:command (keywordize command)
   :exception exception})

(defmethod decode :get
  [parse-result]
 (options/parse-result->command parse-result 1))

(defmethod decode :keys
  [parse-result]
 (options/parse-result->command parse-result 1))

(defmethod decode :ping
  [parse-result]
 (options/parse-result->command parse-result 1))

(defmethod decode :set
   [parse-result]
  (options/parse-result->command parse-result 2))

;; ---------------------------------------------------------------------------- REPL Area

(comment 
  
  "Leave this here.")