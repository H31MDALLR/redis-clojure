(ns redis.commands.dispatch
  (:require
   [redis.encoding.resp2 :as resp2]
   [redis.metrics.updates :as metrics]
   [redis.metrics.latency :as latency]
   [taoensso.timbre :as log]))

;; ---------------------------------------------------------------------------- Layer 0
;; only depends on things outside of this namespace.

(defmulti command-dispatch
  (fn [ctx]
    (get-in ctx [:command-info :command])))

(defmethod command-dispatch :default
  [ctx]
  (log/error ::command-dispatch :unknown-command ctx)
  (throw (ex-info "Unknown command" {:ctx ctx})))

;; ---------------------------------------------------------------------------- Layer 1
;; only depends on things in layer 0

(defn dispatch-command [ctx]
  (let [command (get-in ctx [:command-info :command])
        start-time (System/nanoTime)]
    (try
      (let [result (command-dispatch ctx)
            duration (- (System/nanoTime) start-time)
            duration-usec (/ duration 1000)
            is-error? (and (map? (:response result))
                          (= :error (:type (:response result))))]
        (metrics/record-command! command start-time (not is-error?))
        (latency/record-command-latency! command duration-usec)
        (when is-error?
          (metrics/record-error! :command-error))
        result)
      (catch Exception e
        (let [duration (- (System/nanoTime) start-time)
              duration-usec (/ duration 1000)]
          (metrics/record-command! command start-time false)
          (latency/record-command-latency! command duration-usec)
          (metrics/record-error! :command-error)
          (assoc ctx :response (resp2/error (.getMessage e))))))))

;; ---------------------------------------------------------------------------- REPL
(comment 
  
  ::leave-this-here)
