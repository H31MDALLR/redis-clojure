(ns redis.utils 
  (:require
   [clojure.string :as str]
   [redis.encoding.resp2 :as resp2]))

;; ------------------------------------------------------------------------------------------- Layer 0
(defn keywordize [s] (-> s str/lower-case keyword))