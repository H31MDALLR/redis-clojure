(ns redis.commands.command
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.utils :refer [keywordize]]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Layer 0
;; -------------------------------------------------------- Command Handling

(defmulti exec-command (fn [args] (-> args first str/lower-case keyword)))
(defmethod exec-command :docs [[_ subcommand]]
  (let [docs (->  (io/resource "docs.edn")
                  slurp
                  edn/read-string)]
    (if subcommand 
      (let [docs (->  (io/resource "docs.edn")
                      slurp
                      edn/read-string
                      (get (keywordize subcommand)))]
        (resp2/encode-resp {:array docs}))
      
      ;; all docs
      (let [alldocs (-> docs
                        vals
                        flatten
                        merge
                        vec)]
        (resp2/encode-resp {:array alldocs})))))

;; ------------------------------------------------------------------------------------------- Layer 1
;; -------------------------------------------------------- Dispatch
(defmethod dispatch/command-dispatch :command
  [{:keys [args]}]
  (log/info ::command-dispatch :command {:args args})
  (exec-command args))


;; ------------------------------------------------------------------------------------------- REPL AREA
(comment

  (let [docs (->  (io/resource "docs.edn")
                  slurp
                  edn/read-string
                  vals
                  flatten
                  merge
                  vec)]
    (resp2/encode-resp {:array docs}))
  
  (let [subcommand "SET"
        docs (->  (io/resource "docs.edn")
                  slurp
                  edn/read-string
                  (get (keywordize subcommand)))]
    (resp2/encode-resp {:array docs}))
  
  "Leave this here."
  )