(ns redis.encoding.resp2 
  (:require
    [redis.parsing.resp2 :as parser]))

;; ------------------------------------------------------------------------------------------- RESPN Encoding

(declare encode-resp)

(defn simple-string [text]
  (str "+" text "\r\n"))

(defn error [text]
  (str "-" text "\r\n"))

(defn integer [num]
  (str ":" num "\r\n"))

(defn bulk-string [data]
  (if (nil? data)
    "$-1\r\n"  ; RESP NULL Bulk String
    (str "$" (count data) "\r\n" data "\r\n")))

(defn array [elements]
  (if (nil? elements)
    "*-1\r\n"  ; RESP NULL Array
    (let [encoded-elements (map encode-resp elements)]
      (str "*" (count elements) "\r\n" (apply str encoded-elements)))))

;; ------------------------------------------------------------------------------------------- Public API
(defn encode-resp [element]
  (cond
    (:simple-string element) (simple-string (:simple-string element))
    (:error element)         (error (:error element))
    (:integer element)       (integer (:integer element))
    (:bulk-string element)   (bulk-string (:bulk-string element))
    (:array element)         (array (:array element))
    :else (throw (ex-info "Unknown RESP element" {:element element}))))

(defn ok []
  (simple-string "OK"))

;; ------------------------------------------------------------------------------------------- REPL
(comment 
  (require '[redis.parsing.resp2 :as parser])
  (-> {:array [{:integer 1}
               {:bulk-string "hello"}
               {:simple-string "world"}]}
      encode-resp
      parser/parse-resp)
  (encode-resp {:array [{:integer 1}
                        {:bulk-string "hello"}
                        {:simple-string "world"}]})
  ;; "*3\r\n:1\r\n$5\r\nhello\r\n+world\r\n"

  "Leave this here.")