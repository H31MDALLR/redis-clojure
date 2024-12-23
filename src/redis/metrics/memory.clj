(ns redis.metrics.memory
  (:require [redis.metrics.state :as state]))

(defn init-system-memory! []
  (let [os-bean (java.lang.management.ManagementFactory/getOperatingSystemMXBean)
        total-memory (try
                      (.getTotalPhysicalMemorySize os-bean)
                      (catch Exception _ 0))]
    (state/set-metric! [:memory :total-system-memory] total-memory)))

(defn track-memory-usage! [bytes]
  (state/set-metric! [:memory :used-memory] bytes))

(defn track-rss! []
  (let [runtime (Runtime/getRuntime)
        rss (- (.totalMemory runtime) (.freeMemory runtime))]
    (state/set-metric! [:memory :used-memory-rss] rss)))

(defn track-lua-memory! [bytes]
  (state/set-metric! [:memory :used-memory-lua] bytes))

(defn track-script-memory! [bytes]
  (state/set-metric! [:memory :used-memory-scripts] bytes))

(defn track-peak-memory! []
  (let [current (state/get-metric [:memory :used-memory])
        peak (state/get-metric [:memory :used-memory-peak])]
    (when (> current peak)
      (state/set-metric! [:memory :used-memory-peak] current))))

(defn get-memory-info []
  (state/get-section :memory))

;; Periodic memory tracking
(def memory-tracking-interval 1000)

(defn start-memory-tracking! []
  (init-system-memory!)
  (future
    (while true
      (try
        (track-rss!)
        (track-peak-memory!)
        (Thread/sleep memory-tracking-interval)
        (catch Exception e
          (println "Error in memory tracking:" (.getMessage e)))))))