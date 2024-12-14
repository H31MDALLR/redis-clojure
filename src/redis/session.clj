(ns redis.session 
  (:require
   [clojure.string :as str]
   [redis.protocols.records.sessionmanager :as sessionmanager]
   [redis.protocols.records.atomstore :as sessionstore]
   [redis.time :as time]
   [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------- Layer 0
;; depends only on things outside this file

;; Constructor function
(defn create-session-manager []
  (sessionmanager/->SessionManager (sessionstore/->AtomStore (atom {}) (atom {}))))

(def sm (create-session-manager))

;; ---------------------------------------------------------------------------- REPL AREA

(comment
4 
  (.sessionmanager/add-item sm :1234 [:test :path :to :a :key] "updated-value")
  (.sessionmanager/add-item sm :4444 [:test :path :to :a :key] "different-value")
  (.sessionmanager/delete-item sm :4444 [:test :path :to :a])
  (.sessionmanager/expire-session sm :1234)

  (.sessionmanager/get-item sm :4444 [:test :path :to :a])
  (.sessionmanager/update-item sm :4444 [:test :path :to :a :key] clojure.string/capitalize)
  
  (assoc-in {} (concat [:session :test-id :some-key]) "somevalue")
  (concat [0] [1 2 3])


  (-> (java.util.UUID/randomUUID)
      .toString
      keyword)
  (into {} [{:a "test"} {:a "next"}])
  "Leave this here."
  )