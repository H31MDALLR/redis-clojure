(ns redis.metrics.updates
  (:require [redis.metrics.state :as state]
            [redis.metrics.command-stats :as command-stats]))

(defn record-connection! []
  (state/update-metric! [:stats :total_connections_received] inc))

(defn record-bytes! [direction bytes]
  (state/update-metric! [:stats (case direction
                                 :in :total_net_input_bytes
                                 :out :total_net_output_bytes)]
                       + bytes))

(defn record-error! [error-type]
  (state/update-metric! [:errorstats (str "errorstat_" (name error-type)) :count]
                       (fnil inc 0)))

(defn update-debug! [event value]
  (state/update-metric! [:debug event] + value))

(defn record-command! [command start-time success?]
  (state/update-metric! [:stats :total_commands_processed] inc)
  (command-stats/record-command! command start-time success?))
