(ns redis.rdb.schema.util
  (:require
   [clojure.walk :as walk]
   [taoensso.timbre :as log]))

(defn apply-f-to-key
  [f k path m]
  (walk/postwalk
   (fn [x]
     (if (and (map? x) (contains? x k))
       (update-in x (into [k] path) f)
       x))
   m))

(defn binary-array->string
  [arr]
  (String. (byte-array arr)  java.nio.charset.StandardCharsets/UTF_8))


(defn bytes->string
  [bytes]
  (log/trace ::bytes->string {:bytes bytes})
  (String. bytes))


(defn string->bytes
  [string]
  (log/trace ::string->bytes {:string string})
  (.getBytes string))

(def kewordize-keys (partial apply-f-to-key (comp keyword binary-array->string) :k [:data]))
(def stringize-keys (partial apply-f-to-key binary-array->string))
