(ns redis.storage
  (:require
   [taoensso.timbre :as log]

   [redis.backing-store :refer [backing-store]]
   [redis.parsing.glob :as glob]
   [redis.time :as time]
   [redis.metrics.memory :as memory]))

(defn- estimate-value-size [value]
  (cond
    (string? value) (* 2 (count value))  ; UTF-16 chars
    (bytes? value) (count value)
    (number? value) 8  ; Assuming 64-bit numbers
    :else 16))  ; Default size for other types

(defn- estimate-key-size [k]
  (* 2 (count (str k))))  ; UTF-16 chars

(defn- track-memory-for-value! [k v]
  (let [key-size (estimate-key-size k)
        value-size (estimate-value-size v)]
    (memory/track-memory-usage! (+ key-size value-size))))

(defn- untrack-memory-for-value! [k v]
  (let [key-size (estimate-key-size k)
        value-size (estimate-value-size v)]
    (memory/track-memory-usage! (- (+ key-size value-size)))))

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
        (when value 
          (untrack-memory-for-value! k value))
        (swap! backing-store update-in [db :database] dissoc k)
        nil)
      value)))

(defn store [db k v]
  (log/trace ::store {:k k
                      :v v})
  
  (let [old-value (get-in @backing-store [db :database k :value])
        value (time/update-last-write-access v)]
    (when old-value
      (untrack-memory-for-value! k old-value))
    (track-memory-for-value! k (:value value))
    (log/trace ::store {:updated-value value})
    (swap! backing-store 
           update-in 
           [db :database]
           assoc
           k
           value)))

(defn get-all-dbs [] (keys @backing-store))

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
