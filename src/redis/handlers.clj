(ns redis.handlers
  (:require
   [gloss.core :as gloss]
   [gloss.io :as io]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [redis.commands.command]
   [redis.commands.config]
   [redis.commands.dispatch :as dispatch]
   [redis.commands.echo]
   [redis.commands.error]
   [redis.commands.get]
   [redis.commands.keys]
   [redis.commands.info]
   [redis.commands.ping]
   [redis.commands.replconf]
   [redis.commands.set]
   [redis.decoder :as decoder]
   [redis.encoding.resp2 :as resp2]
   [redis.metrics.clients :as client-metrics]
   [redis.parsing.resp2 :as parser]
   [redis.session :as session]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Defs
(log/set-min-level! :trace)


;; ---------------------------------------------------------------------------- Layer 0
;; only depends on things outside of this namespace.

;; -------------------------------------------------------- Handler

(defn handler
  [ctx]
  (log/info ::handler ctx)
  (->> ctx
       parser/parse-resp
       decoder/decode
       dispatch/dispatch-command))


;; ---------------------------------------------------------------------------- Layer 1
;; only depends on things in layer 0

(defn handle-message [context]
  (try
    (let [{:keys [response]} (handler context)]
      (log/info ::handle-message {:response response})
      ;; Track output buffer size
      (when (string? response)
        (client-metrics/record-client-buffer!  (count (.getBytes response "UTF-8")) 
                                               :output))
      (s/put! (:socket context) response))
    (catch Exception e
      (log/error ::handle-message {:anomaly :anomalies/fault
                                   :error e}))))

;; ---------------------------------------------------------------------------- Layer 2
;; only depends on things in layer 1

(defn handle-connection [socket info]
  (log/info ::handle-connection {:connection info})
  (let [fingerprint (hash info)
        session-id  (.get-or-create! session/sm fingerprint)
        client-info {:id       fingerprint
                     :addr     (:remote-addr info)
                     :port     (:server-port info)
                     :name     nil
                     :age      0
                     :idle     0
                     :flags    #{}
                     :db       0
                     :tracking false
                     :timeout  0}]
    (log/info ::handle-connection {:client-info client-info})

    ;; Record the new client connection using the passed in tracker
    (client-metrics/record-client-connection! client-info)

    ;; Add cleanup on socket close
    (s/on-closed socket #(client-metrics/record-client-disconnection! session-id))
    
    (let [context {:connection-info info
                   :session-id      session-id
                   :socket          socket}]
      (s/consume
       (fn [raw-message]
         (try
           (let [message (String. raw-message "UTF-8")
                 ;; Track input buffer size
                 _       (client-metrics/record-client-buffer! (count raw-message) :input)
                 context (assoc context :message message)]
             (log/info ::handle-connection {:message message})
             (handle-message context))
           (catch Exception e
             (log/error ::handle-connection {:error e})
             (s/close! socket)))) ;; Close on error
       socket))))
;; ------------------------------------------------------------------------------------------- REPL AREA

(comment
  (do
    (log/set-min-level! :trace)
    (.get-or-create! session/sm (hash {:test "test"}))
    (.get-item session/sm :3a2c2455-fa92-4efe-a3e6-c54831f60926 [:db])

    (def set-command "*7\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\ntest\r\n$2\r\nPX\r\n$2\r\nNX\r\n$7\r\nKEEPTTL\r\n$3\r\nGET\r\n")
    (def docs-command "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")

    (def set-banana "*5\r\n$3\r\nSET\r\n$6\r\nbanana\r\n$5\r\napple\r\n$2\r\npx\r\n$3\r\n100\r\n")
    (def get-banana  "*2\r\n$3\r\nGET\r\n$6\r\nbanana\r\n")
    (def ping-command "*1\r\n$4\r\nPING\r\n")
    (def echo-command "*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n")
    (def replconf-command "*3\r\n$7\r\nREPLCONF\r\n$7\r\nlistening-port\r\n$5\r\n6379\r\n"))

  
  (let [info        {:remote-addr "127.0.0.1"
                     :ssl-session nil
                     :server-port 6379
                     :server-name "localhost"}
        fingerprint (hash info)
        context     {:message    replconf-command
                     :session-id (.get-or-create! session/sm fingerprint)}]
    (handler context))
  
  (ns-unalias *ns* 'encoder)
  (handler echo-command)
  (handler docs-command)
  (handler ping-command)
  (handler set-banana)
  (-> set-command parser/parse-resp decoder/decode)

  (let [[one two] '("test" "stuff")]
    [one two])
  
   ;; ----------------------------------------------------- ALEPH examples 
  
  (def protocol
    (gloss/compile-frame
     (gloss/finite-frame :uint32
                         (gloss/string :utf-8))))
  (defn chain-test
    [f]
    (fn [s info]

      (log/info "New connection from:" info)
  ;; `socket` is a duplex stream, you can read and write to it
      (d/loop []
        (-> (s/take! s ::none)
            (d/chain  (fn [msg]
                        (if (= ::none msg)
                          ::none
                          (d/future (f msg))))
                      (fn [msg]
                        (s/put! s msg))
                      (fn [result]
                        (when result
                          (d/recur))))
            (d/catch
             (fn [ex]
               (s/put! s (resp2/error ex))
               (s/close! s)))))))

  (defn wrap-duplex-stream
    [protocol s]
    (let [out (s/stream)]
      (s/connect
       (s/map #(io/encode protocol %) out)
       s)
      (s/splice
       out
       (io/decode-stream s protocol))))

  "leave this here.")