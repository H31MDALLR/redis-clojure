(ns redis.metrics.keyspace
  (:require [redis.metrics.state :as state]))

(defn update-keyspace-stats! [db-id keys expires avg-ttl]
  (state/set-metric! [:keyspace (str "db" db-id)]
                     {:keys keys
                      :expires expires
                      :avg_ttl avg-ttl}))

(defn increment-expires! [db-id]
  (state/update-metric! [:keyspace (str "db" db-id) :expires] inc))

(defn decrement-expires! [db-id]
  (state/update-metric! [:keyspace (str "db" db-id) :expires] #(max 0 (dec %))))

(defn increment-keys! [db-id]
  (state/update-metric! [:keyspace (str "db" db-id) :keys] inc))

(defn decrement-keys! [db-id]
  (state/update-metric! [:keyspace (str "db" db-id) :keys] #(max 0 (dec %))))

(defn update-avg-ttl! [db-id new-avg-ttl]
  (state/set-metric! [:keyspace (str "db" db-id) :avg_ttl] new-avg-ttl))

(defn get-keyspace-metrics []
  (state/get-section :keyspace))