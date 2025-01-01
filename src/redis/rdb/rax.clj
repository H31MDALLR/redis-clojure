(ns redis.rdb.rax 
  (:require
   [gloss.core :as gloss]))

(defn parse-rax
  []
  (gloss/compile-frame 
   (gloss/ordered-map
    :type :rax
    :radix-tree (parse-string))))
