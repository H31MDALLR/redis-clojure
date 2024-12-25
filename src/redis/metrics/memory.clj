(ns redis.metrics.memory
  (:require
   [redis.metrics.state :as state]
   [taoensso.timbre :as log]
   [clojure.core.async :as async :refer [<! timeout go-loop]]))


;; ---------------------------------------------------------------------------- Layer 0
;; depends only on things outside this file

;; -------------------------------------------------------- defs
; Periodic memory tracking
(def memory-tracking-interval 1000)
(def initial-backoff 1000)  ; 1 second
(def max-backoff 300000)    ; 5 minutes
(def ^:private running? (atom true))
(def ^:private current-backoff (atom initial-backoff))

;; -------------------------------------------------------- tracking fns
(defn get-memory-info []
  (state/get-section :memory))

(defn init-system-memory! []
  (let [os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean)
        total-memory (try
                       (.getTotalPhysicalMemorySize os-bean)
                       (catch Exception _ 0))]
    (state/set-metric! [:memory :total_system_memory] total-memory)))


(defn track-memory-usage! [bytes]
  (state/set-metric! [:memory :used_memory] bytes))

(defn track-rss! []
  (let [runtime (Runtime/getRuntime)

        rss (- (.totalMemory runtime) (.freeMemory runtime))]
    (state/set-metric! [:memory :used_memory_rss] rss)))

(defn track-lua-memory! [bytes]
  (state/set-metric! [:memory :used_memory_lua] bytes))

(defn track-script-memory! [bytes]
  (state/set-metric! [:memory :used_memory_scripts] bytes))

(defn track-peak-memory! []
  (let [current (or (state/get-metric [:memory :used_memory]) 0)
        peak (or (state/get-metric [:memory :used_memory_peak]) 0)]
    (when (> current peak)
      (state/set-metric! [:memory :used_memory_peak] current))))

;; ---------------------------------------------------------------------------- Layer 1
;; depends on things in layer 0

(defn increase-backoff! []
  (swap! current-backoff #(min max-backoff (* 2 %))))


(defn reset-backoff! []
  (reset! current-backoff initial-backoff))

(defn stop-memory-tracking! []
  (reset! running? false))

;; ---------------------------------------------------------------------------- Layer 2
;; depends on things in layer 1

(defn start-memory-tracking! []
  (init-system-memory!)
  (reset! running? true)
  (reset-backoff!)
  (go-loop []
    (when @running?
      (try
        (track-rss!)
        (track-peak-memory!)
        (reset-backoff!)
        (<! (timeout memory-tracking-interval))
        (catch Exception e
          (log/error e "Error in memory tracking, backing off for" @current-backoff "ms")
          (<! (timeout @current-backoff))
          (increase-backoff!)))
      (recur))))
