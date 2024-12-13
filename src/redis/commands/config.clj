(ns redis.commands.config
  (:require
   [taoensso.timbre :as log]

   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.utils :refer [keywordize]]
   [redis.config :as config]))

;; ---------------------------------------------------------------------------- Layer 0


;; -------------------------------------------------------- Command Handling

(defmulti exec-command
  (fn [subcommand _] subcommand))

;; TBD - support glob style pattern search over our keys
;;  CONFIG GET takes multiple arguments, which are glob-style patterns. 
;;  Any configuration parameter matching any of the patterns are 
;;  reported as a list of key-value pairs.
                                          
(defmethod exec-command :get
  [_ key-coll]
  (log/trace ::exec-command :get {:keys key-coll})
  (let [config-values (reduce (fn [acc curr] 
                                (let [value (config/get-value [:redis/config (keywordize curr)])]
                                  (-> acc
                                      (conj curr)
                                      (conj value))))
                              []
                              key-coll)
        get-response  (resp2/coll->resp2-array config-values)]
    (log/trace ::exec-command :get {:response get-response})
    get-response))

(defmethod exec-command :help [_ _])
(defmethod exec-command :resetstat [_  opts])
(defmethod exec-command :rewrite [_  opts])
(defmethod exec-command :set [_  kv-coll]
  (doseq [[k v] (partition 2 kv-coll)]
    (config/write [:redis/config (keywordize k)] v))
  (resp2/ok))

(defmethod exec-command :default [subcommand options]
  (log/error ::exec-command {:anomaly :anomalies/not-found
                             :subcommand subcommand
                             :options options})
  (resp2/error (str "Unknown option given: " subcommand)))

;; ------------------------------------------------------------------------------------------- Layer 1
;; -------------------------------------------------------- Dispatch
(defmethod dispatch/command-dispatch :config
  [{:keys [command-info] :as ctx}] 
  (let [{:keys [subcommand options]} command-info]
    (log/info ::command-dispatch :config {:subcommand subcommand
                                          :options    options})
    (assoc ctx :response (exec-command (keywordize subcommand) options))))


;; ----------------------------------------------------------------------------REPL AREA
(comment 
  (ns-unalias *ns* 'exec-command)
  (-> :redis/config config/get-value (get (keyword "maxA")))

  (config/get-value [:redis/config "maxA"])
  (dispatch/command-dispatch )
  (exec-command :set ["maxA" "test" "maxB" "test2" "maxC" :stuff])
  (exec-command :get ["dir" "dbfilename"])
  (exec-command ["HELP"] {})

  (let [config-values (doall (map (fn [k] (config/get-value [:redis/config (keywordize k)])) 
                                  ["dir" "dbfilename"]))]
     (resp2/bulk-string config-values))

  "Leave this here."
  )