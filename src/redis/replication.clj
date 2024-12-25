(ns redis.replication
  (:refer-clojure :exclude [replicate])
  (:require
   [aleph.tcp :as tcp]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [redis.encoding.commands :as encoding]
   [redis.metrics.replication :as replication-metrics]
   [redis.metrics.state :as state]
   [redis.parsing.resp2 :as parser]
   [redis.session :as session]
   [redis.snowflake :as snowflake]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------------- Layer 0
;; depends on nothing in this namespace
(defn on-error 
  [socket expected actual]
  (log/error ::replica-handshake :handshake-failed {:expected expected :actual actual})
  (s/close! socket)
  (throw (ex-info "Handshake failed" {:expected expected :actual actual})))

(defn check-response
  [expected actual]
  (let [result  (as-> actual $
                      (String. $ "UTF-8")
                      (parser/parse-resp {:message $})
                      (:parse-result $))]
    [result (= expected result)]))

(defn replicate-from [master-port {:keys [host port] :as replica-info}]
  (log/info ::begin-streaming {:master-port master-port
                               :replica-info replica-info})
  (d/future
    (try
      (d/loop []
        (let [socket @(tcp/client {:host host :port port})]
          (log/info ::begin-streaming :connected-to-master)

          ;; Set up error handling for the stream
          (s/on-closed socket #(log/info ::begin-streaming :connection-closed))

          (d/chain
           (s/consume (fn [data]
                        (try
                          (log/debug ::begin-streaming :received-data {:data data})
                          ;; Here you would process and apply the received changes
                          ;; For example, parse RESP2 format and execute commands
                          (catch Exception e
                            (log/error ::begin-streaming :processing-error e))))
                      socket)
            ;; When consume completes (socket closed), attempt reconnection
           (fn [_]
             (log/warn ::begin-streaming :reconnecting)
             (Thread/sleep 1000)
             (d/recur)))))
      (catch Exception e
        (log/error ::begin-streaming :fatal-error e)
        (throw e)))))

(defn replicate-to [replica-port {:keys [id host port] :as replica-info}]
  (log/info ::begin-streaming {:replica-port replica-port
                              :replica-info replica-info})
  (let [write-stream (s/stream)]
    ;; Start connection management in separate future
    (d/future
      (try
        (d/loop []
          (let [socket @(tcp/client {:host host :port port})]
            (log/info ::begin-streaming {:replica-connected id})
            
            ;; Set up error handling for the stream
            (s/on-closed socket #(log/info ::begin-streaming :connection-closed))
            
            ;; Connect write-stream to new socket
            (s/connect write-stream socket)
            
            ;; Wait for socket to close before recurring
            @(d/chain
              (s/on-closed socket #(log/info ::begin-streaming :connection-closed))
              (fn [_]
                (log/warn ::begin-streaming :reconnecting)
                (Thread/sleep 1000)
                (d/recur)))))
        (catch Exception e
          (log/error ::begin-streaming :fatal-error e)
          (throw e))))
    
    ;; Return the write stream immediately
    write-stream))

(defn set-replication-data
  "Set the replication data for the server."
  [role {:keys [host port]}]
  (log/info ::set-replication-data {:role role
                                    :host host
                                    :port port})
  (replication-metrics/update-connected-slaves! 0)
  (replication-metrics/update-master-id! (snowflake/generate-snowflake))
  (replication-metrics/update-role! (name role))
  (replication-metrics/update-master-host! host)
  (replication-metrics/update-master-port! port))

;; ------------------------------------------------------------------------------------------------- Layer 1
;; depends on only layer 0 in this namespace

(defn check-and-continue
  [actual expected socket next-command]
  (let [[result succeeded?]  (check-response expected actual)]
    (log/info ::check-and-continue {:prior-response-succeeded? succeeded?})
    
    (if succeeded? 
      (let [accepted?  @(s/put! socket next-command)]
        (log/info ::check-and-continue {:put-command-accepted? accepted?})
        (if accepted?
          (s/take! socket)
          (on-error socket expected result)))
      (on-error socket expected result))))

(defn check-and-close [element expected socket]
  (let [succeeded?  (check-response expected element)]
    (if succeeded?
      (d/future (s/close! socket))
      (on-error socket expected element))))

;; ------------------------------------------------------------------------------------------------- Layer 2
;; depends on only layer 1 in this namespace

(defn replica-handshake [{:keys [host port] :as replica-info} recieve-port]
  (log/info "Initiating handshake with master server at" host ":" recieve-port)
  (d/chain
   (tcp/client {:host host :port port})            ;; returns Deferred<Stream>
   (fn [socket]
     (log/info ::replica-handshake :initiating-handshake)
     (s/put! socket (encoding/ping))               ;; returns Deferred<Boolean>
     (d/chain
      (s/take! socket ::none)                     ;; returns Deferred<ByteBuf or String or ...>
      #(check-and-continue % '([:SimpleString "PONG"]) socket (encoding/replconf "listening-port" recieve-port))
      #(check-and-continue % '([:SimpleString "OK"]) socket (encoding/replconf "capa" "psync2"))
      #(check-and-continue % '([:SimpleString "OK"]) socket (encoding/psync "?" "-1"))
      #(check-and-close % '([:SimpleString "OK"]) socket)))
   (d/catch
    Exception
    (fn [e]
      (log/error ::replica-handshake :handshake-failed e)
      (throw (ex-info "Failed to establish connection with master"
                      {:host  host
                       :port  recieve-port
                       :error (.getMessage e)}))))))

(defn replica-handshake-old [{:keys [host port]}]
  (log/info "Initiating handshake with master server at" host ":" port)
  (try
    (let [socket       (tcp/client {:host host
                                    :port port})
          ping-cmd     (encoding/ping)
          replconf-cmd (encoding/replconf "listeningport" port)
          capa-cmd     (encoding/replconf "capa" "psync2")]
      (log/info ::replica-handshake :initiating-handshake)

      @(s/put! socket ping-cmd)
      
      (d/chain
       (s/take! socket ::none)
       #(check-and-continue % '([:SimpleString "OK"]) socket replconf-cmd)
       #(check-and-continue % '([:SimpleString "OK"]) socket capa-cmd)
       #(check-and-close % '([:SimpleString "OK"]) socket)
       (d/catch Exception  #(log/error ::replica-handshake :handshake-failed %))))
    
    (catch Exception e
      (log/error "Failed to connect to master server:" e)
      (throw (ex-info "Failed to establish connection with master" 
                      {:host  host 
                       :port  port 
                       :error (.getMessage e)})))))

;; ------------------------------------------------------------------------------------------------- Layer 3
;; depends on only layer 2 in this namespace

(defn initialize-replication! [{:keys [host] :as replica-info} port]
  (let [role (if (string? host) :slave :master)]
    (set-replication-data role replica-info)
    (case role
      :slave (replica-handshake replica-info port)
      :master (log/info ::initialize-replication! :master-initialized))))

(defn begin-replication [session-id]
  (let [replica-port (.get-item session/sm session-id [:listening-port])
        fingerprint  (.get-item session/sm session-id [:fingerprint])
        replica-info (state/get-metric [:clients :client_infos fingerprint])]
    (replicate-from replica-port replica-info)))

(comment
  (initialize-replication! {:host "127.0.0.1" :port 6379}) 
  ::leave-this-here)
