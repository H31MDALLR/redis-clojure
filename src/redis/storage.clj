(ns redis.storage
  (:require
   [redis.time :as time]
   [taoensso.timbre :as log]))

(def backing-store (atom {}))

(defn add-db [db-id db]
  (swap! backing-store assoc db-id db))

(defn get-aux-values [db-id & k])

(defn retrieve [db k]
  (let [{:keys [value expiry] :as data} (get-in @backing-store [db :database k])]
    (log/trace ::retrieve {:k k
                           :v value})
    (when (seq value) (swap! backing-store
                             assoc
                             k
                             (time/update-last-read-access data)))

    (if (and expiry (time/expired? expiry))
      (do
        (log/trace ::retrieve {:expired? true
                               :k k
                               :v value})
        (swap! backing-store dissoc k)
        nil)
      value)))

(defn store [db k v]
  (log/trace ::store {:k k
                      :v v})
  (swap! backing-store 
         assoc-in 
         [db :database k] 
         (time/update-last-write-access v)))

(comment

  "Leave this here.")