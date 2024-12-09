(ns redis.session 
  (:require
   [redis.time :as time]
   [redis.session :as session]
   [clojure.string :as str]))

;; ---------------------------------------------------------------------------- Layer 0
;; depends only on things outside this file

;; -------------------------------------------------------- defs
(def sessions (atom {}))

;; ---------------------------------------------------------------------------- Layer 1
;; depends only on layer 1

;; -------------------------------------------------------- CRUD for sessions

(defn create-session
  []
  (let [id (-> (java.util.UUID/randomUUID)
               .toString
               keyword)
        session {:db 0}]
    (swap! sessions assoc-in [:sessions id] (time/update-last-write-access session))
    id))

(defn expire-session [id]
  (swap! sessions dissoc id))

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
  "Leave this here.")