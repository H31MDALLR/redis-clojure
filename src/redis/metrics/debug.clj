(ns redis.metrics.debug
  (:require [redis.metrics.state :as state]))

(defn record-event! [event-type]
  (state/update-metric! [:debug :events event-type] (fnil inc 0)))

(defn record-timing! [event-type duration-ns]
  (let [duration-ms (/ duration-ns 1000000.0)]
    (state/update-metric! [:debug :timings event-type]
                         (fn [stats]
                           (let [stats (or stats {:count 0
                                                :total 0
                                                :min ##Inf
                                                :max ##-Inf})]
                             (-> stats
                                 (update :count inc)
                                 (update :total + duration-ms)
                                 (update :min min duration-ms)
                                 (update :max max duration-ms)))))))

(defn record-memory! [category bytes]
  (state/update-metric! [:debug :memory category] (fnil + 0) bytes))

(defn get-debug-metrics []
  (state/get-section :debug))

(defn reset-debug-metrics! []
  (state/set-metric! [:debug] {}))