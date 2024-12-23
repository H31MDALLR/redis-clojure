(ns redis.metrics.updates
  (:require [redis.metrics.state :as state]
            [redis.metrics.command-stats :as command-stats]))

(defn record-connection! []
  (state/update-metric! [:stats :total-connections-received] inc))

(defn record-bytes! [direction bytes]
  (state/update-metric! [:stats (case direction
                                 :in :total-net-input-bytes
                                 :out :total-net-output-bytes)]
                       + bytes))

(defn record-error! [error-type]
  (state/update-metric! [:errorstats (str "errorstat_" (name error-type)) :count]
                       (fnil inc 0)))

(defn update-debug! [event value]
  (state/update-metric! [:debug event] + value))

(defn record-command! [command start-time success?]
  (state/update-metric! [:stats :total-commands-processed] inc)
  (command-stats/record-command! command start-time success?))