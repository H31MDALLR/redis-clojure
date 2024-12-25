(ns redis.encoding.commands 
  (:require
   [redis.encoding.resp2 :as resp2]))

(defn ping 
  "Encode a PING command"
  []
  (resp2/coll->resp2-array ["PING"]))

(defn psync
  "Encode a PSYNC command"
  [replication-id offset]
  (resp2/coll->resp2-array ["PSYNC" replication-id offset]))

(defn replconf
  "Encode a REPLCONF command"
  [option value]
  (resp2/coll->resp2-array ["REPLCONF" option value]))

(defn sync 
  "Encode a SYNC command"
  []
  (resp2/coll->resp2-array ["SYNC"]))

(defn rdb-save [])

(defn rdb-load [])

(defn rdb-save-done [])

(defn rdb-load-done [])

(defn rdb-load-continue [])

(defn rdb-load-continue-done [])

(defn rdb-load-continue-continue [])
