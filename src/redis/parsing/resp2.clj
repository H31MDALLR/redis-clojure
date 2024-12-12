(ns redis.parsing.resp2
  (:require
   [instaparse.core :as insta]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- EBNF Parser

(def resp-parser
  (insta/parser
   "
    <RESP>        = SimpleString / Error / Integer / BulkString / Array
    SimpleString = <'+'> TEXT <CRLF>
    Error        = <'-'> TEXT <CRLF>
    Integer      = <':'> DIGITS <CRLF>
    <BulkString>   = <'$'> (<LENGTH> <CRLF> DATA <CRLF> | NIL)
    <Array>        = <'*'> <LENGTH> <CRLF> (RESP | NIL)*

    <TEXT>         = #'[^\r\n]+'
    <DIGITS>       = #'[0-9]+'
    <DATA>       = #'[^\r\n]*'
    LENGTH       = DIGITS
    <NIL>         = <'-1'> <CRLF>

    CRLF         = <'\\r\\n'>
    "))

;; Parse a RESP input

(defn parse-resp 
  [{:keys [message] :as ctx}]
  (log/trace ::parse-resp (pr-str message))
  (let [result (resp-parser message)]
    (if (insta/failure? result)
      (do
        (log/error ::parse-resp (ex-info "Parsing failed" {:error (insta/get-failure result)}))
        (assoc ctx :response ["ERROR" (str "Error parsing command: " message)]))
      (assoc ctx :parse-result result))))

;; ------------------------------------------------------------------------------------------- REPL AREA

(comment
  (require '[clojure.pprint :as pprint])

  ;; Example RESP inputs
  (let [examples ["*5\r\n$3\r\nSET\r\n$5\r\napple\r\n$9\r\npineapple\r\n$2\r\npx\r\n$3\r\n100\r\n"
                  "*5\r\n$3\r\nSET\r\n$5\r\ngrape\r\n$5\r\napple\r\n$2\r\npx\r\n$3\r\n100\r\n"
                  "+OK\r\n"
                  "-Error message\r\n"
                  ":12345\r\n"
                  "$6\r\nfoobar\r\n"
                  "$0\r\n\r\n"
                  "$-1\r\n"
                  "*2\r\n+OK\r\n:1000\r\n"
                  "*2\r\n$3\r\nGET\r\n$5\r\napple\r\n"
                  "*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n"]]
    (for [example examples]
      (parse-resp {:message example})))

  (do
    ;; Example RESP inputs
    (def array "*3\r\n+OK\r\n$4\r\nPING\r\n+-ERR unknown command\r\n")
    (def bulk-string "$6\r\nfoobar\r\n")
    (def docs-command "*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n")
    (def error "-ERR unknown command\r\n")
    (def get-command "*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$6\r\nHello!\r\n")
    (def get-test "*2\r\n$3\r\nGET\r\n$5\r\napple\r\n")
    (def integer ":1000\r\n")
    (def null-bulk-string "$0\r\n\r\n")
    (def ping-command "*1\r\n$4\r\nPING\r\n")
    (def set-command "*7\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$4\r\ntest\r\n$2\r\nPX\r\n$2\r\nNX\r\n$7\r\nKEEPTTL\r\n$3\r\nGET\r\n")
    (def simple-string "+OK\r\n")

    
    (def px-error "*5\r\n$3\r\nSET\r\n$6\r\norange\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n"))

  (let [examples [docs-command
                  get-command
                  ping-command
                  set-command]]
    (for [cmd examples]
      (-> {:message cmd}
          parse-resp
          :parse-result)))
  
;; Parse them
  (parse-resp {:message px-error})
  (parse-resp {:message set-command})
  (parse-resp {:message docs-command})
  (parse-resp {:message ping-command})

  (parse-resp {:message simple-string})
;; => [:SimpleString "OK"]
  
  (parse-resp {:message error})
;; => [:Error "ERR unknown command"]
  
  (parse-resp {:message integer})
;; => [:Integer "1000"]
  
  (parse-resp {:message bulk-string})
;; => [:BulkString "foobar"]
  
  (parse-resp {:message ping-command})

  (parse-resp {:message null-bulk-string})
;; => [:BulkString :NULL]
  
  (parse-resp {:message get-command})
  
  (parse-resp {:message array})
;; => [:Array [:SimpleString "OK"] [:Error "ERR unknown command"]]
  )