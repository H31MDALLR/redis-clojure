(ns redis.core
  (:gen-class)
  (:require
   [redis.lifecycle :as lifecycle]))

;; ------------------------------------------------------------------------------------------- Main

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; You can use print statements as follows for debugging, they'll be visible when running tests.
  (println "Logs from your program will appear here!")
  ;; Uncomment this block to pass the first stage
  (lifecycle/run)
  )

;; ------------------------------------------------------------------------------------------- REPL AREA

(comment 
  
  "leave this here."
  )