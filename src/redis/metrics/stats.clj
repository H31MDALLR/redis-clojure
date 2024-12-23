(ns redis.metrics.stats
  (:require [redis.metrics.state :as state]))

(defn increment-connections! []
  (state/update-metric! [:stats :total_connections_received] inc))

(defn increment-commands! []
  (state/update-metric! [:stats :total_commands_processed] inc))

(defn add-input-bytes! [bytes]
  (state/update-metric! [:stats :total_net_input_bytes] + bytes))

(defn add-output-bytes! [bytes]
  (state/update-metric! [:stats :total_net_output_bytes] + bytes))

(defn increment-rejected-connections! []
  (state/update-metric! [:stats :rejected_connections] inc))

(defn increment-expired-keys! []
  (state/update-metric! [:stats :expired_keys] inc))

(defn increment-evicted-keys! []
  (state/update-metric! [:stats :evicted_keys] inc))

(defn increment-keyspace-hits! []
  (state/update-metric! [:stats :keyspace_hits] inc))

(defn increment-keyspace-misses! []
  (state/update-metric! [:stats :keyspace_misses] inc))

(defn update-pubsub-channels! [count]
  (state/set-metric! [:stats :pubsub_channels] count))

(defn update-pubsub-patterns! [count]
  (state/set-metric! [:stats :pubsub_patterns] count))

(defn get-stats-metrics []
  (state/get-section :stats))