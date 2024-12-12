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
                             assoc-in
                             [db :database k]
                             (time/update-last-read-access data)))

    (if (and (seq expiry) (time/expired? expiry))
      (do
        (log/trace ::retrieve {:expired? true
                               :db db
                               :k k
                               :v value})
        (swap! backing-store update-in [db :database] dissoc k)
        nil)
      value)))

(defn store [db k v]
  (log/trace ::store {:k k
                      :v v})
  
  (let [value (time/update-last-write-access v)]
    (log/trace ::store {:updated-value value})
    (swap! backing-store 
           update-in 
           [db :database]
           assoc
           k
           value)))

(comment
  (-> @backing-store
      (get 0)
      :database
      (get "mykey"))

  (let [store (atom {})
        kv {:k "mykey", :v {:value "Hello!"}}
        v (time/update-last-write-access (:v kv))]
    (swap! store assoc-in [0 :database (:k kv)] v)
    @store)

  (store 0 "test2" "another value")

  "Leave this here.")