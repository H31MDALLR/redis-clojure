(ns redis.storage
  (:require
   [redis.time :as time]
   [taoensso.timbre :as log]))


(def backing-store (atom {}))

(defn retrieve [k]
  (let [{:keys [expiry]
         :as   value} (get @backing-store k)]
    (log/trace ::retrieve {:k k
                             :v value})
    (when (seq value) (swap! backing-store 
                             assoc 
                             k 
                             (time/update-last-read-access value)))

    (if (and expiry (time/expired? expiry))
      (do 
        (log/trace ::retrieve {:expired? true
                               :k k
                               :v value})
        (swap! backing-store dissoc k)
       nil)
      value)))

(defn store [k v]
  (log/trace ::store {:k k 
                      :v v})
  (swap! backing-store assoc k (time/update-last-write-access v)))

(comment

  "Leave this here.")