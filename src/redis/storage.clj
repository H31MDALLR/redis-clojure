(ns redis.storage
  (:require
   [taoensso.timbre :as log]

   [redis.backing-store :refer [backing-store]]
   [redis.parsing.glob :as glob]
   [redis.time :as time]))

(defn add-db [db-id db]
  (swap! backing-store assoc db-id db))

(defn get-aux-values [db-id & k])

(defn find-keys 
  ([pattern]
   (reduce (fn [accum k]
             (conj accum [k (find-keys k pattern)])) 
           []
           (keys @backing-store)))
  ([db pattern]
   (let [key-coll (-> @backing-store
                      (get-in [db :database])
                      keys)]
     (glob/match-keys pattern key-coll))))

(defn retrieve [db k]
  (let [{:keys [value expiry]
         :as   data} (get-in @backing-store [db :database k])]
    (log/trace ::retrieve {:k k
                           :v value})
    (when (seq value) (swap! backing-store
                             assoc-in
                             [db :database k]
                             (time/update-last-read-access data)))

    (if (and (instance? java.time.Instant expiry) (time/expired? expiry))
      (do
        (log/trace ::retrieve {:expired? true
                               :db       db
                               :k        k
                               :v        value})
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
  (require '[java-time.api :as jt])
  (-> @backing-store
      (get 0)
      :database
      (get "expires_ms_precision"))
  
  (swap! backing-store
         update-in [0 :database]
         assoc 
         "blueberry"
         {:expiry (jt/instant 1640995200000N), :kind :RDB_TYPE_STRING, :value "apple"})
  
  (swap! backing-store
         update-in [0 :database]
         assoc 
         "orange"
         {:expiry (jt/instant 1640995200000N), :kind :RDB_TYPE_STRING, :value "raspberry"})

  (let [store (atom {})
        kv {:k "mykey", :v {:value "Hello!"}}
        v (time/update-last-write-access (:v kv))]
    (swap! store assoc-in [0 :database (:k kv)] v)
    @store)
  
  (def test (find-keys 0 "*"))

  (let [{:keys [value expiry] :as data} (get-in @backing-store [0 :database "banana"])]
    (if (and (instance? java.time.Instant expiry) (time/expired? expiry))
      (log/trace ::retrieve {:expired? true 
                             :v value})
      data))
  (retrieve 0 "banana")


  (store 0 "test2" "another value")

  "Leave this here.")