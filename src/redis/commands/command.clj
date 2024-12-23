(ns redis.commands.command
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [taoensso.timbre :as log]
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.utils :refer [keywordize]]))

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

(defmulti exec-command (fn [ctx] 
                        (let [subcommand (-> ctx :command-info :subcommand)]
                          (log/trace ::exec-command-dispatch 
                                   {:subcommand subcommand
                                    :type (type subcommand)
                                    :command-info (:command-info ctx)})
                          (if (keyword? subcommand)
                            subcommand
                            (keyword (str/lower-case subcommand))))))

(defmethod exec-command :docs [{:keys [args]
                               :as   ctx}]
  (log/trace ::exec-command-docs {:args args})
  (let [response     (if (seq args) 
                      (select-keys docs args) 
                      (all-docs))
        encoded-resp (resp2/encode-resp {:array response})]
    (assoc ctx :response encoded-resp)))

(defmethod exec-command :default [{:keys [command-info] :as ctx}]
  (let [subcommand (:subcommand command-info)]
    (log/error ::exec-command {:anomaly :anomalies/not-found
                              :subcommand subcommand
                              :command-info command-info})
    (assoc ctx :response (resp2/error (str "Unknown option given: " subcommand)))))

;; ---------------------------------------------------------------------------- Layer 1
;; depends only on layer 1


;; -------------------------------------------------------- Dispatch
(defmethod dispatch/command-dispatch :command
  [ctx]
  (log/info ::command-dispatch :command ctx)
  (exec-command ctx))


;; ---------------------------------------------------------------------------- REPL AREA
(comment
(def exec-command nil)
(ns-unalias *ns* 'command-dispatch)
  
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