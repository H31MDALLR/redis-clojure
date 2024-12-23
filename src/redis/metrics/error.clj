(ns redis.metrics.error
  (:require [redis.metrics.state :as state]))

(defn record-error! [error-type]
  (state/update-metric! [:errorstats (str "errorstat_" (name error-type)) :count]
                       (fnil inc 0)))

(defn get-error-metrics []
  (state/get-section :errorstats))