(ns redis.metrics.persistence
  (:require [redis.metrics.state :as state]))

(defn set-loading! [loading?]
  (state/set-metric! [:persistence :loading] loading?))

(defn increment-changes! []
  (state/update-metric! [:persistence :changes_since_save] inc))

(defn start-bgsave! []
  (state/set-metric! [:persistence :bgsave_in_progress] true)
  (state/set-metric! [:persistence :bgsave_start_time] (System/currentTimeMillis)))

(defn complete-bgsave! [status]
  (let [start-time (state/get-metric [:persistence :bgsave_start_time])
        duration (- (System/currentTimeMillis) start-time)]
    (state/set-metric! [:persistence :bgsave_in_progress] false)
    (state/set-metric! [:persistence :last_save_time] (System/currentTimeMillis))
    (state/set-metric! [:persistence :last_bgsave_status] status)
    (state/set-metric! [:persistence :last_bgsave_duration] duration)))

(defn get-persistence-metrics []
  (state/get-section :persistence))