(ns redis.metrics.cluster
  (:require [redis.metrics.state :as state]))

(defn update-cluster-state! [state]
  (state/set-metric! [:cluster :state] state))

(defn update-slots-assigned! [count]
  (state/set-metric! [:cluster :slots-assigned] count))

(defn update-slots-ok! [count]
  (state/set-metric! [:cluster :slots-ok] count))

(defn update-slots-pfail! [count]
  (state/set-metric! [:cluster :slots-pfail] count))

(defn update-slots-fail! [count]
  (state/set-metric! [:cluster :slots-fail] count))

(defn update-known-nodes! [count]
  (state/set-metric! [:cluster :known-nodes] count))

(defn update-size! [size]
  (state/set-metric! [:cluster :size] size))

(defn update-current-epoch! [epoch]
  (state/set-metric! [:cluster :current-epoch] epoch))

(defn get-cluster-metrics []
  (state/get-section :cluster)) 