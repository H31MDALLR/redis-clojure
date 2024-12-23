(ns redis.metrics.slowlog
  (:require [redis.metrics.state :as state]))

(def ^:private max-slowlog-entries 128)
(def ^:private slowlog-threshold-micros 10000) ; 10ms default threshold

(defn record-slow-command! [command args duration-usec timestamp]
  (when (> duration-usec slowlog-threshold-micros)
    (state/update-metric! [:slowlog :entries]
                         (fn [entries]
                           (let [entries (or entries [])
                                 new-entry {:id (count entries)
                                          :timestamp timestamp
                                          :duration duration-usec
                                          :command command
                                          :args args}]
                             (vec (take max-slowlog-entries
                                      (conj entries new-entry))))))))

(defn get-slowlog []
  (state/get-metric [:slowlog :entries]))

(defn get-slowlog-len []
  (count (get-slowlog)))

(defn reset-slowlog! []
  (state/set-metric! [:slowlog :entries] [])) 