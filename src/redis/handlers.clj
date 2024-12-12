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
   [redis.commands.ping]
   [redis.commands.set]
   [redis.decoder :as decoder]
   [redis.encoding.resp2 :as resp2]
   [redis.parsing.resp2 :as parser]
   [redis.session :as session]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Defs
(log/set-min-level! :trace)
(defn handler
  [ctx]
  (log/info ::handler ctx)
  (->> ctx
       parser/parse-resp
       decoder/decode
       dispatch/command-dispatch))

;; ------------------------------------------------------------------------------------------- Handler

(def protocol
  (gloss/compile-frame
   (gloss/finite-frame :uint32
                       (gloss/string :utf-8))))


(defn wrap-duplex-stream
  [protocol s]
  (let [out (s/stream)]
    (s/connect
     (s/map #(io/encode protocol %) out)
     s)
    (s/splice
     out
     (io/decode-stream s protocol))))

;; ------------------------------------------------------------------------------------------- Network I/O

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

;; ------------------------------------------------------------------------------------------- Exposed Handlers

(defn handle-message [context]
  (try
    (let [{:keys [response]} (handler context)]
      (log/info "Sending response:" response)
      (s/put! (:socket context) response))
    (catch Exception e
      (log/error e "Error handling message"))))

(defn handle-connection [socket info]
  (log/info "New connection from:" info)
  (let [session-id (session/get-or-create-session (hash info))
        context    {:connection-info info
                    :session-id      session-id
                    :socket          socket}]
    (s/consume
     (fn [raw-message]
       (try
         (let [message (String. raw-message "UTF-8")
               context (assoc context :message message)] ;; Decode incoming bytes
           (log/info "Received message:" message)
           (handle-message context))
         (catch Exception e
           (log/error e "Error decoding message" e)
           (s/close! socket)))) ;; Close on error
     socket)))

;; ------------------------------------------------------------------------------------------- REPL AREA

(comment
  (do
    (log/set-min-level! :trace)

    (def set-command "*7\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\ntest\r\n$2\r\nPX\r\n$2\r\nNX\r\n$7\r\nKEEPTTL\r\n$3\r\nGET\r\n")
    (def docs-command "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
    (def get-command "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$6\r\nHello!\r\n")
    (def ping-command "*1\r\n$4\r\nPING\r\n")
    (def echo-command "*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n"))

  
   (let [info {:remote-addr "127.0.0.1", :ssl-session nil, :server-port 6379, :server-name "localhost"}
         fingerprint (hash info)
         context    {:message get-command
                    :session-id      (session/get-or-create-session fingerprint)}]
     (handler context))
     
  (ns-unalias *ns* 'encoder)
  (handler echo-command)
  (handler docs-command)
  (handler ping-command)
  (handler get-command)
  (-> set-command parser/parse-resp decoder/decode)

  (let [[one two] '("test" "stuff")]
    [one two])

  "leave this here.")