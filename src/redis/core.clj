(ns redis.core
  (:gen-class)
  (:require
   [clojure.java.io :as io]

   [taoensso.timbre :as log]

   [redis.commands.command :as command]
   [redis.commands.error]
   [redis.commands.ping]
   [redis.commands.set]
   [redis.decoder :as decoder]
   [redis.parser :as parser])
  (:import
   [java.net ServerSocket]))

;; ------------------------------------------------------------------------------------------- Defs

(defn read-with-crlf [input-stream]
  (loop [buffer (byte-array 1024)
         output []]
    (let [read-count (.read input-stream buffer)]
      (if (neg? read-count)
        output
        (recur buffer (conj output (String. buffer 0 read-count)))))))

(defn read-lines [reader]
  (loop [output ""]
    (let [ready? (.ready reader)]
      (if ready? 
        (recur (str output  (.readLine reader) "\r\n"))
        (do 
          (log/info ::read-lines {:output output})
          output)))))

;; ------------------------------------------------------------------------------------------- Network I/O

(defn receive-message
  "Read a line of textual data from the given socket"
  [socket]
  (let [r   (io/reader (.getInputStream socket))
        msg (read-lines r)]
    (log/info ::recieve-message {:msg r})
    msg))

(defn send-message
  "Send the given string message out over the given socket"
  [socket msg]
  (log/info ::send-message {:msg msg})
  (let [writer (io/writer (.getOutputStream socket))]
    (.write writer (str msg))
    (.flush writer)))

(defn serve [port handler]
  (with-open [server-sock (ServerSocket. port)]
    ;; Since the tester restarts your program quite often, setting SO_REUSEADDR
    ;; ensures that we don't run into 'Address already in use' errors
    (. server-sock (setReuseAddress true))

   (with-open [sock (.accept server-sock)]
    (let [msg-in (receive-message sock)
          _  (log/info ::serve {:msg msg-in})

          msg-out (handler msg-in)]
      (send-message sock msg-out)))))

;; ------------------------------------------------------------------------------------------- Handler

(defn handler
  [& args]
  (log/info ::handler {:args args})

  (->> args
       first
       parser/parse-resp
       decoder/decode
       command/command))

;; ------------------------------------------------------------------------------------------- Main

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  ;; You can use print statements as follows for debugging, they'll be visible when running tests.
  (println "Logs from your program will appear here!")
  ;; Uncomment this block to pass the first stage
  (serve 6379 handler)
  )

;; ------------------------------------------------------------------------------------------- REPL AREA

(comment 
  (do 
    (log/set-min-level! :trace)
    
    (def get-command "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$6\r\nHello!\r\n")
    (def ping-command "*1\r\n$4\r\nPING\r\n")
    (def set-command "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n"))
  
  (handler ping-command)
  (handler get-command)
  (-> get-command parser/parse-resp decoder/decode)
  (serve 6379 handler)

  (let [[one two] '("test" "stuff")]
    [one two])

  "leave this here."
  )