(ns redis.metrics.replication
  (:require [redis.metrics.state :as state]))

(defn update-connected-slaves! [count]
  (state/set-metric! [:replication :connected_slaves] count))

(defn update-master-host! [host]
  (state/set-metric! [:replication :master_host] host))

(defn update-master-port! [port]
  (state/set-metric! [:replication :master_port] port))

(defn update-master-id! [arg1]
  (state/set-metric! [:replication :master_replid] arg1))

(defn update-master-link-status! [status]
  (state/set-metric! [:replication :master_link_status] status))

(defn update-master-repl-offset! [offset]
  (state/set-metric! [:replication :master_repl_offset] offset))

(defn update-role! [role]
  (state/set-metric! [:replication :role] role))

(defn get-replication-metrics []
  (state/get-section :replication))
