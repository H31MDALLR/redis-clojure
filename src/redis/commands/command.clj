(ns redis.commands.command
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.utils :refer [keywordize]]
   [taoensso.timbre :as log]))


;; ---------------------------------------------------------------------------- Layer 0
;; depends on nothing in this file
(def ^:private docs
  (->  (io/resource "docs.edn")
       slurp
       edn/read-string))

;; ---------------------------------------------------------------------------- Layer 1
;; depends only on layer 0
(defn all-docs []
  (-> docs
      vals
      flatten
      merge
      vec))

(defn subcommand-docs [subcommand]
  (get docs (keywordize subcommand)))

;; -------------------------------------------------------- Command Handling

(defmulti exec-command (fn [ctx] (-> ctx 
                                     :command 
                                     :subcommand)))

(defmethod exec-command :docs [{:keys [command]
                                :as   ctx}]
  (let [{:keys [subcommand]} command
        response             (if subcommand 
                               (subcommand-docs subcommand) 
                               (all-docs))
        encoded-resp         (resp2/encode-resp {:array response})]
    (assoc ctx :response encoded-resp)))

;; ---------------------------------------------------------------------------- Layer 1
;; depends only on layer 1

;; -------------------------------------------------------- Dispatch
(defmethod dispatch/command-dispatch :command
  [ctx]
  (log/info ::command-dispatch :command ctx)
  (exec-command ctx))


;; ---------------------------------------------------------------------------- REPL AREA
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