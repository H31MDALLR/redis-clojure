(ns redis.core
  (:gen-class)
  (:require
   [clojure.tools.cli :refer [parse-opts]]
   [redis.lifecycle :as lifecycle]
   [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------- Layer 0
;; depends only on things outside this file
(defn numeric-string? [s]
  (try
    (log/trace ::numeric-string? {:s s})
    (let [num (java.lang.Integer/parseInt s)]
      num)
    (catch Exception _
      false)))

;; ---------------------------------------------------------------------------- CLI options
;; TBD: add validation that these are valid paths.

(def cli-options
  [["-dir" "--dir DIRECTORY" "Directory path where the RDB database is stored"
    :validate [string?]]
   ["-dbfilename" "--dbfilename FILENAME" "Filename of an RDB database file."
    :validate [string?]]
   ["-p" "--port PORT" "A port number the database will listen on."
    :validate [numeric-string?]
    :assoc-fn (fn [m k v]  (assoc m k (Integer/parseInt v)))]])

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

  (parse-opts '("--port" "6389") cli-options)
  (parse-opts '("--dir" "resources/rdb" "--dbfilename" "dump.rdb") cli-options)
  "leave this here.")