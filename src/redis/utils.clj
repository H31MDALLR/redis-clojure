(ns redis.utils
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]))

;; ------------------------------------------------------------------------------------------- Layer 0
(defn keywordize [s] (-> s str/lower-case keyword))

(defn apply-f-to-key
  "Apply a function to values for map key k at any depth"
  [m k f]
  (walk/postwalk
   (fn [x]
     (if (and (map? x) (contains? x k))
       (update x k f)
       x))
   m))