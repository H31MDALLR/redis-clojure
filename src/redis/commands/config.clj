(ns redis.commands.config

  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.utils :refer [keywordize]]
   [taoensso.timbre :as log]
   [redis.parsing.options :as options]
   [redis.config :as config]))

;; ------------------------------------------------------------------------------------------- Layer 0
;; -------------------------------------------------------- Command Handling

(defmulti exec-command
  (fn [defaults _]
    (-> defaults first str/lower-case keyword)))

(defmethod exec-command :get
  [[_ key] _]
  (log/trace ::exec-command :get {:key key})
  (let [config-value (-> :persistence/rdb 
                         config/get-value 
                         (get (keyword key)))]
    (resp2/bulk-string config-value)))

(defmethod exec-command :help [[_ opts]])
(defmethod exec-command :resetstat [[_ opts]])
(defmethod exec-command :rewrite [[_ opts]])
(defmethod exec-command :set [[_ opts]])

;; ------------------------------------------------------------------------------------------- Layer 1
;; -------------------------------------------------------- Dispatch
(defmethod dispatch/command-dispatch :config
  [{:keys [defaults options]}]
  (log/info ::command-dispatch :config {:subcommand (first defaults)
                                        :options    options})
  (exec-command defaults options))


;; ------------------------------------------------------------------------------------------- REPL AREA
(comment 
  (ns-unalias *ns* 'exec-command)
  (-> :persistence/rdb config/get-value (get (keyword "dir")))
  
  (exec-command ["GET" "dir"] {})
  (exec-command ["HELP"] {})

  "Leave this here.")