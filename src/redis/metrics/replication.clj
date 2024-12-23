(ns redis.metrics.replication
  (:require [redis.metrics.state :as state]))

(defn update-role! [role]
  (state/set-metric! [:replication :role] role))

(defn update-master-host! [host]
  (state/set-metric! [:replication :master-host] host))

(defn update-master-port! [port]
  (state/set-metric! [:replication :master-port] port))

(defn update-master-link-status! [status]
  (state/set-metric! [:replication :master-link-status] status))

(defn update-connected-slaves! [count]
  (state/set-metric! [:replication :connected-slaves] count))

(defn get-replication-metrics []
  (state/get-section :replication))