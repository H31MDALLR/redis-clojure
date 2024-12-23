(ns redis.metrics.cpu
  (:require [redis.metrics.state :as state])
  (:import [com.sun.management OperatingSystemMXBean]
           [java.lang.management ManagementFactory]))

(def ^OperatingSystemMXBean os-bean 
  (ManagementFactory/getOperatingSystemMXBean))

(defn update-cpu-stats! []
  (let [system-cpu (try 
                     (.getSystemCpuLoad os-bean)
                     (catch Exception _ -1))
        process-cpu (try 
                     (.getProcessCpuLoad os-bean)
                     (catch Exception _ -1))]
    (state/set-metric! [:cpu :used-cpu-sys] (if (neg? system-cpu) 0 (* 100 system-cpu)))
    (state/set-metric! [:cpu :used-cpu-user] (if (neg? process-cpu) 0 (* 100 process-cpu)))))

(def cpu-tracking-interval 1000) ; 1 second

(defn start-cpu-tracking! []
  (future
    (while true
      (try
        (update-cpu-stats!)
        (Thread/sleep cpu-tracking-interval)
        (catch Exception e
          (println "Error in CPU tracking:" (.getMessage e)))))))

(defn get-cpu-metrics []
  (state/get-section :cpu))