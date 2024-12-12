(ns redis.session 
  (:require
   [clojure.string :as str]
   
   [taoensso.timbre :as log]

   [redis.time :as time]
   [redis.session :as session]))

;; ---------------------------------------------------------------------------- Layer 0
;; depends only on things outside this file

;; -------------------------------------------------------- defs
(def sessions (atom {}))
(def fingerprint-session-map (atom {}))

;; default of 24 hours for now.
(def expiry-default 86400)

(defn should-expire? [session-id]
  (let [last-write (-> @sessions session-id :last-write-time)]
    ()))

(defn create-session [fingerprint]
  (let [id (-> (java.util.UUID/randomUUID)
                   .toString
                   keyword)
            expiry (time/set-expiry-time {:seconds [expiry-default]})
            session (merge {:fingerprint fingerprint 
                            :db 0}
                           expiry)]
        (swap! fingerprint-session-map assoc fingerprint id)
        (swap! sessions assoc-in [id] session)
        (log/trace ::create-session {:created id})
        id))

;; ---------------------------------------------------------------------------- Layer 1
;; depends only on layer 1

;; -------------------------------------------------------- CRUD for sessions

(defn expire-session [id]
  (log/trace ::expire-session id)
  (let [fingerprint (:id @sessions)]
    (swap! sessions dissoc id)
    (swap! fingerprint-session-map dissoc fingerprint)))

(defn get-or-create-session
  [fingerprint]
  (log/trace ::create-session :enter)
  (let [extant-session (get @fingerprint-session-map fingerprint)]
    (if extant-session
      (if (time/expired? (-> @sessions extant-session :expiry))
        (do 
          (log/trace ::get-or-create-session {:renewing extant-session})
          (expire-session extant-session)
          (create-session fingerprint))
        extant-session)
      (create-session fingerprint))))

;; --------------------------------------------------------ßCRUD on items

(defn add-item 
  [id path v]
  (let [add-keyfn (fn [m]
                    (-> m
                        (assoc-in (concat [id] path) v)
                        (update id time/update-last-write-access)))]
    (swap! sessions add-keyfn)))

(defn delete-item 
  [id path]
  (swap! sessions update-in (concat [id] (butlast path)) dissoc (last path)))

(defn get-item 
  [id path]
  (get-in @sessions (concat [id] path)))

(defn update-item 
  [id path f]
  (let [update-fn (fn [m]
                    (-> m
                        (update-in (concat [id] path) f)
                        (update id time/update-last-write-access)))]
    (swap! sessions update-fn)))


;; ---------------------------------------------------------------------------- REPL AREA

(comment

  (def test-session (get-or-create-session (hash {:remote-addr "127.0.0.1", :ssl-session nil, :server-port 6379, :server-name "localhost"})))
  (-> @sessions test-session :expiry :expiry)

  (add-item :1234 [:test :path :to :a :key] "updated-value")
  (add-item :4444 [:test :path :to :a :key] "different-value")
  (delete-item :4444 [:test :path :to :a])
  (expire-session :1234)

  (get-item :4444 [:test :path :to :a])
  (update-item :4444 [:test :path :to :a :key] clojure.string/capitalize)
  
  (assoc-in {} (concat [:session :test-id :some-key]) "somevalue")
  (concat [0] [1 2 3])


  (-> (java.util.UUID/randomUUID)
      .toString
      keyword)
  (into {} [{:a "test"} {:a "next"}])
  "Leave this here."
  )