(ns redis.commands.set
  (:require
   [taoensso.timbre :as log]

   [redis.commands.dispatch :as dispatch]
   [redis.commands.impl.get :as get]
   [redis.encoding.resp2 :as resp2]
   [redis.storage :as storage]
   [redis.time :as time]))

;; ------------------------------------------------------------------------------------------- Data

(def expiry-keys #{:milliseconds
                   :seconds
                   :unix-time-seconds
                   :unix-time-milliseconds})

;; ------------------------------------------------------------------------------------------- Util fns
(defn mutually-exclusive-options [nx xx options]
  (let [ expiry-keys (select-keys options expiry-keys)]
    (cond
                               ;; defensive - note - parsing is broken if these get through.
      (and nx xx)
      (do (log/error "NX and XX options returned from parsing. Parsing is broken!")
          (resp2/error "NX and XX options are mutually exclusive"))
      
      (> 1 (count expiry-keys))
      (do (log/error "Mutually exlusive expiration options made it through parsing!" (filter some? expiry-keys))
          (resp2/error (str "Expiration options are mutually exclusive, given: " (filter some? expiry-keys)))))))

(defn set-kv
  "Store a value v at location k"
  [k v]
  (swap! storage/store assoc k (time/update-write-access-time v)))

;; ------------------------------------------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :set
  [{:keys [defaults options]}]
  (let [[k v]               defaults
        {:keys [get nx xx]} options 
        get-result          get/key->value
        option-errors       (mutually-exclusive-options nx xx options)]
    (log/trace ::dispatch :set {:k k
                                :v v})

    (if option-errors
      option-errors

      ;; Expiration, NX and XX options parsing 
      (let [value          (time/map->map-with-expiry v options)
            encoded-result (cond
                             (and nx (empty? get-result)) (resp2/bulk-string (set-kv k value))
                             (and nx (seq get-result)) (resp2/bulk-string nil)
                             (and xx (empty? get-result)) (resp2/bulk-string nil)
                             (and xx (seq get-result)) (resp2/bulk-string (set-kv k value))
                             :else (do (set-kv k value)
                                       (resp2/ok)))]

        ;; GET option
        (if get 
          (resp2/bulk-string get-result)
          encoded-result)))))


;; ------------------------------------------------------------------------------------------- REPL AREA
(comment
  
  "Leave this here.")