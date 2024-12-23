(ns redis.metrics.stats
  (:require [redis.metrics.state :as state]))

(defn increment-connections! []
  (state/update-metric! [:stats :total-connections-received] inc))

(defn increment-commands! []
  (state/update-metric! [:stats :total-commands-processed] inc))

(defn add-input-bytes! [bytes]
  (state/update-metric! [:stats :total-net-input-bytes] + bytes))

(defn add-output-bytes! [bytes]
  (state/update-metric! [:stats :total-net-output-bytes] + bytes))

(defn increment-rejected-connections! []
  (state/update-metric! [:stats :rejected-connections] inc))

(defn increment-expired-keys! []
  (state/update-metric! [:stats :expired-keys] inc))

(defn increment-evicted-keys! []
  (state/update-metric! [:stats :evicted-keys] inc))

(defn increment-keyspace-hits! []
  (state/update-metric! [:stats :keyspace-hits] inc))

(defn increment-keyspace-misses! []
  (state/update-metric! [:stats :keyspace-misses] inc))

(defn update-pubsub-channels! [count]
  (state/set-metric! [:stats :pubsub-channels] count))

(defn update-pubsub-patterns! [count]
  (state/set-metric! [:stats :pubsub-patterns] count))

(defn get-stats-metrics []
  (state/get-section :stats))