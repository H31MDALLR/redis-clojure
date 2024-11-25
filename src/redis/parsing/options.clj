(ns redis.parsing.options
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [redis.utils :refer [keywordize]]
   [taoensso.timbre :as log]))

;; ------------------------------------------------------------------------------------------- Ruleset management
(defn load-ruleset
  "Loads a ruleset from an EDN file and attaches logic functions to each command."
  [filename]
  (let [ruleset (-> filename
                    io/resource
                    slurp
                    edn/read-string)]
    ruleset))

(def ruleset (-> "redis-rules.edn"
                 (load-ruleset)))

(defn caseless-compare [x y]
  (= (-> x str/lower-case)
     (-> y str/lower-case)))

;; ------------------------------------------------------------------------------------------- Parsing
(defn matches? [rules arg]
  ;; (and ... ) here as null check.
  (filter #(or (and (:token %) 
                    (caseless-compare (:token %) arg))
               (and 
                (:name %)
                (caseless-compare (:name %) arg)))
          rules))

(defn find-matching-rules [rules options]
  (loop [acc       {}
         remaining options]
    (if (empty? remaining)
      acc
      (let [[arg & _]      remaining
            ;; Match rule by token or name
            matching-rules (matches? rules arg)
            rule           (first matching-rules)]
        (if rule
          (let [size              (:size rule)
                to-consume        (take size remaining)
                remaining-options (drop size remaining)]
            (if (= (count to-consume) size)
              (recur (update acc (:name rule) #(conj (or % []) {:args to-consume
                                                                :rule rule}))
                     remaining-options)
              (throw (ex-info "Insufficient arguments for rule" {:rule rule
                                                                 :args to-consume}))))
          (throw (ex-info "Unknown argument" {:arg arg})))))))

(defn validate-exclusivity-rules
  "Make sure that if a group is exclusive, we do not have multiple arguments from that group"
  [parsed]
  (doseq [[name args] parsed]
    (let [parent-type (get-in (first args) [:rule :parent-type])]
      (when (= parent-type "oneof")
          ;; Ensure group exclusivity
        (when (> (count args) 1)
          (throw (ex-info "Mutually exclusive arguments in `oneof` group"
                          {:name name
                           :args args})))))))

(defn parse-data->options
  [parsed]
  (reduce-kv
   (fn [acc name args]
     (let [rule           (:rule (first args))        ; Get the rule metadata
           values         (mapcat #(rest (:args %)) args) ; Extract argument values, skipping tokens
           coerced-values (case (keyword (:type rule)) ; Coerce based on the rule's type
                            :double (->> values (map #(Double/parseDouble %)) vec)
                            :integer (->> values (map #(Integer/parseInt %)) vec)
                            :string (vec values)
                            values)] ; Default to raw values
       (assoc acc (keyword name) (if (seq coerced-values) coerced-values [])))) ; Handle empty values
   {}
   parsed))


(defn parse-result->command
  "Transforms a flat string collection of command arguments into a structured representation
   based on the ruleset, while validating the arguments and coercing values based on types.\r\n
   Input:\n
     args - flat list of arguments (e.g., [\"SET\" \"mykey\" \"10\" \"NX\" \"EX\" \"10000\" \"GET\"])
     rules - ruleset for the command
     defaults-count - number of default arguments
   Output:
     A map with formatted defaults and options."
  [[command & args] defaults-count]
  (let [[defaults options] (split-at defaults-count args) ; Separate defaults and options
        rules              (get ruleset (keywordize command))
        parsed             (find-matching-rules rules options)]

    (validate-exclusivity-rules parsed)

    ;; Transform parsed options with coercion
    (let [options (parse-data->options parsed)]

      ;; Return final structured result
      {:command  (keywordize command)
       :defaults (vec defaults) ; Convert defaults to vector
       :options  options})))



;; ------------------------------------------------------------------------------------------- REPL
(comment

  (do
    (log/set-min-level! :trace)
    ;; Define rules for Redis options

    (def ruleset (-> "redis-rules.edn"
                     (load-ruleset)))

    (def command-docs-command ["COMMAND" "DOCS" "SET"])
    (def set-command ["SET" "mykey" "value" "NX" "EX" "10000" "GET"])
    (def set-error '("SET" "orange" "pear" "PX" "100")))

  (some #(when (= (:token %) "NX") %) (:set ruleset))

  (try
    (parse-result->command command-docs-command 1)
    (catch clojure.lang.ExceptionInfo e
      (ex-data e)))
  (try
    (parse-result->command set-error 2)
    (catch clojure.lang.ExceptionInfo e
      (ex-data e)))

  (try
    (-> ruleset
        :set
        (find-matching-rules ["NX" "EX" "10000" "GET"]))
    (catch clojure.lang.ExceptionInfo e
      (ex-data e)))

  (->> ruleset
       :command)

  {:command  :set,
   :defaults ["mykey" "value"],
   :options  {:nx  []
              :ex  [10000]
              :get []}}

  "leave this here.")