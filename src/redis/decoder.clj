(ns redis.decoder 
  (:require
   [taoensso.timbre :as log]
   
   [redis.parsing.options :as options]
   [redis.utils :refer [keywordize]]))

;; ------------------------------------------------------------------------------------------- Layer 0


;; ------------------------------------------------------------------------------------------- Layer 1
;; -------------------------------------------------------- Decode Interface

(defmulti decode  #(-> % :parse-result first keywordize))

(defmethod decode :command
  [{:keys [parse-result]
    :as   ctx}]
  (let [[command & args] parse-result
        cmd-map          {:command    (keywordize command)
                          :subcommand (-> args first keywordize)
                          :args       (rest args)}]
    (assoc ctx :command-info cmd-map)))

;; special
(defmethod decode :config
  [{:keys [parse-result]
    :as   ctx}]
  (let [[command & args]       parse-result
        [subcommand & options] args
        command-info           {:command    (keywordize command)
                                :subcommand subcommand
                                :options    options}]
    (assoc ctx :command-info command-info)))

(defmethod decode :echo
  [ctx]  
  (options/parse-result->command ctx 1))

(defmethod decode :error
  [{:keys [parse-result]
    :as   ctx}]
  (let [[command exception] parse-result]
    (assoc ctx 
           :command-info 
           {:command   (keywordize command)
            :exception exception})))

(defmethod decode :get
  [ctx]
  (options/parse-result->command ctx 1))

(defmethod decode :info
  [{:keys [parse-result]
    :as   ctx}]
  (log/info ::decode :info ctx)
  (let [[command & args] parse-result]
    (assoc ctx :command-info {:command (keywordize command)
                             :args args})))

(defmethod decode :keys
  [ctx]
  (options/parse-result->command ctx 1))

(defmethod decode :ping
  [ctx]
 (options/parse-result->command ctx 1))

(defmethod decode :set
   [ctx]
 (options/parse-result->command ctx 2))

;; ---------------------------------------------------------------------------- REPL Area

(comment 
  (ns-unalias *ns* 'decode)

  (def docs-command {:parse-result '("COMMAND" "DOCS")})
  (-> docs-command :parse-result first keywordize)
  (decode {:parse-result '("COMMAND" "DOCS")})
  
  (do
    (log/set-min-level! :trace)

    (def set-command "*7\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\ntest\r\n$2\r\nPX\r\n$2\r\nNX\r\n$7\r\nKEEPTTL\r\n$3\r\nGET\r\n")
    (def docs-command ["*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n" '("COMMAND" "DOCS")])
    (def get-command "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$6\r\nHello!\r\n")
    (def ping-command "*1\r\n$4\r\nPING\r\n")
    (def echo-command "*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n"))
  
  (for [parser-result ['("COMMAND" "DOCS")
                       '("SET" "mykey" "Hello!")
                       '("PING") 
                       '("SET" "mykey" "test" "PX" "1000" "NX" "KEEPTTL" "GET")]]
    (let [context    {:parse-result parser-result}]
      (decode context)))

  "Leave this here."
  )