(ns redis.core
  (:gen-class)
  (:require
   
   [aleph.tcp :as tcp]
   [gloss.core :as gloss]
   [gloss.io :as io]
   [manifold.deferred :as d]
   [manifold.stream :as s]
   [taoensso.timbre :as log]
   
   [redis.commands.dispatch :as dispatch]
   [redis.commands.command]
   [redis.commands.echo]
   [redis.commands.error]
   [redis.commands.get]
   [redis.commands.ping]
   [redis.commands.set]
   [redis.decoder :as decoder]
   [redis.parsing.resp2 :as parser]
   [redis.encoding.resp2 :as resp2]))

;; ------------------------------------------------------------------------------------------- Defs
(log/set-min-level! :trace)
(defn handler
  [msg]
  (log/info ::handler {:args msg})

  (->> msg
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

(defn handle-message [message socket]
  (try
    (let [response (handler message)]
      (log/info "Sending response:" response)
      (s/put! socket response))
    (catch Exception e
      (log/error e "Error handling message"))))

(defn handle-connection [socket info]
  (log/info "New connection from:" info)
  (s/consume
   (fn [raw-message]
     (try
       (let [message (String. raw-message "UTF-8")] ;; Decode incoming bytes
         (log/info "Received message:" message)
         (handle-message message socket))
       (catch Exception e
         (log/error e "Error decoding message" e)
         (s/close! socket)))) ;; Close on error
   socket))

(defn start-server [port]
  (log/info "Starting Aleph server on port:" port)
  (tcp/start-server
   (fn [socket info]
     (handle-connection socket info))
   {:port port}))


;; ------------------------------------------------------------------------------------------- Main

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; You can use print statements as follows for debugging, they'll be visible when running tests.
  (println "Logs from your program will appear here!")
  ;; Uncomment this block to pass the first stage
  (start-server 6379)
  )

(.addShutdownHook (Runtime/getRuntime)
                  (Thread. #(log/info "Shutdown hook triggered")))

;; ------------------------------------------------------------------------------------------- REPL AREA

(comment 
  (do 
    (log/set-min-level! :trace)

    (def set-command "*7\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\ntest\r\n$2\r\nPX\r\n$2\r\nNX\r\n$7\r\nKEEPTTL\r\n$3\r\nGET\r\n")
    (def docs-command "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")   
    (def get-command "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$6\r\nHello!\r\n")
    (def ping-command "*1\r\n$4\r\nPING\r\n")
    (def echo-command "*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n"))
  
  (ns-unalias *ns* 'encoder)
  (handler echo-command)
  (handler docs-command) 
  (handler ping-command)
  (handler get-command)
  (-> set-command parser/parse-resp decoder/decode)

  (let [[one two] '("test" "stuff")]
    [one two])

  "leave this here."
  )