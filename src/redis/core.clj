(ns redis.core
  (:gen-class)
  (:require
   [clojure.tools.cli :refer [parse-opts]]
   [redis.lifecycle :as lifecycle]
   [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------- CLI options
(def cli-options
  [["-dir" "--dir DIRECTORY" "Directory path where the RDB database is stored"
    :validate [string?]]
   ["-dbfilename" "--dbfilename FILENAME" "Filename of an RDB database file."
    :validate [string?]]])

;; ---------------------------------------------------------------------------- Main

(defn -main
  "I don't do a whole lot ... yet."
  [& args]

  (println "Logs from your program will appear here!")
  (log/trace ::main {:args args})
  (let [parsed-args (parse-opts args cli-options)]
    (lifecycle/run parsed-args)))

;; ---------------------------------------------------------------------------- REPL AREA

(comment 

  (parse-opts '("--dir" "resources/rdb" "--dbfilename" "dump.rdb") cli-options)
  "leave this here."
  )