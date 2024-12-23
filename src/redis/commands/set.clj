(ns redis.commands.set
  (:require
   [taoensso.timbre :as log]

   [redis.commands.dispatch :as dispatch]
   [redis.encoding.resp2 :as resp2]
   [redis.storage :as storage]
   [redis.time :as time]
   [redis.session :as session]))

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
      
      (> (count expiry-keys) 1)
      (do (log/error "Mutually exlusive expiration options made it through parsing!" (filter some? expiry-keys))
          (resp2/error (str "Expiration options are mutually exclusive, given: " (filter some? expiry-keys)))))))

(defn store-and-get [db k value nx xx]
  (let [get-result          (storage/retrieve db k)]
    (storage/store db k value)
    (cond
      (and nx (empty? get-result)) (resp2/bulk-string get-result)
      (and nx (seq get-result)) (resp2/bulk-string nil)
      (and xx (empty? get-result)) (resp2/bulk-string nil)
      (and xx (seq get-result)) (resp2/bulk-string get-result))))

(defn impl-set [{:keys [command-info session-id]
                 :as   ctx}]
  (let [{:keys [defaults options]} command-info
        [k v]                      defaults
        {:keys [get nx xx]}        options 
        db                         (.get-item! session/sm session-id [:db])

        ;; grab it before write ops if needed
        get-result                 (when get (resp2/bulk-string (storage/retrieve db k)))
        option-errors              (mutually-exclusive-options nx xx options)]
    (log/trace ::dispatch :set {:defaults defaults
                                :options  options})

    (if option-errors
      option-errors

      ;; Expiration, NX and XX options parsing 
      (let [expiry         (time/map->map-with-expiry v options)
            value          (into {:value v} expiry)
            db             (.get-item! session/sm session-id [:db])
            encoded-result (if (or nx xx) 
                             (store-and-get db k value nx xx)
                             (do (storage/store db k value)
                                 (resp2/ok)))]
        
        (log/trace ::set {:encoded-result encoded-result})
        
        ;; GET option
        (if get 
          (assoc ctx :response get-result)
          (assoc ctx :response encoded-result))))))


;; ------------------------------------------------------------------------------------------- Dispatch

(defmethod dispatch/command-dispatch :set
  [ctx]
  (impl-set ctx))


;; ------------------------------------------------------------------------------------------- REPL AREA
(comment
  
  "Leave this here.")