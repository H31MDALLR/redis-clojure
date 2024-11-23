(ns redis.encoder 
  (:require
    [redis.parser :as parser]))

;; ------------------------------------------------------------------------------------------- RESPN Encoding

(declare encode-resp)

(defn encode-simple-string [text]
  (str "+" text "\r\n"))

(defn encode-error [text]
  (str "-" text "\r\n"))

(defn encode-integer [num]
  (str ":" num "\r\n"))

(defn encode-bulk-string [data]
  (if (nil? data)
    "$-1\r\n"  ; RESP NULL Bulk String
    (str "$" (count data) "\r\n" data "\r\n")))

(defn encode-array [elements]
  (if (nil? elements)
    "*-1\r\n"  ; RESP NULL Array
    (let [encoded-elements (map encode-resp elements)]
      (str "*" (count elements) "\r\n" (apply str encoded-elements)))))

;; ------------------------------------------------------------------------------------------- Public API
(defn encode-resp [element]
  (cond
    (:simple-string element) (encode-simple-string (:simple-string element))
    (:error element)         (encode-error (:error element))
    (:integer element)       (encode-integer (:integer element))
    (:bulk-string element)   (encode-bulk-string (:bulk-string element))
    (:array element)         (encode-array (:array element))
    :else (throw (ex-info "Unknown RESP element" {:element element}))))

;; ------------------------------------------------------------------------------------------- REPL
(comment 
  (require '[redis.parser :as parser])
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