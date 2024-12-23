(ns redis.metrics.persistence
  (:require [redis.metrics.state :as state]))

(defn set-loading! [loading?]
  (state/set-metric! [:persistence :loading] loading?))

(defn increment-changes! []
  (state/update-metric! [:persistence :changes-since-save] inc))

(defn start-bgsave! []
  (state/set-metric! [:persistence :bgsave-in-progress] true)
  (state/set-metric! [:persistence :bgsave-start-time] (System/currentTimeMillis)))

(defn complete-bgsave! [status]
  (let [start-time (state/get-metric [:persistence :bgsave-start-time])
        duration (- (System/currentTimeMillis) start-time)]
    (state/set-metric! [:persistence :bgsave-in-progress] false)
    (state/set-metric! [:persistence :last-save-time] (System/currentTimeMillis))
    (state/set-metric! [:persistence :last-bgsave-status] status)
    (state/set-metric! [:persistence :last-bgsave-duration] duration)))

(defn get-persistence-metrics []
  (state/get-section :persistence))