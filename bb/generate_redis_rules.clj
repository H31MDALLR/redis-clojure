#!/usr/bin/env bb

(require '[babashka.process :refer [shell]]
         '[cheshire.core :as json])

(defn rule [size valid? process]
  {:size size :valid? valid? :process process})

(defn infer-size
  "Infers the size of an argument based on its token and type."
  [token type]
  (if (nil? token) 1
      (case type
        "pure-token" 1   ; Standalone token (e.g., NX, GET)
        "key" 1          ; Key arguments are standalone
        "double" 2       ; Token + value (e.g., timeout 100.50)
        "integer" 2      ; Token + value (e.g., EX 10000)
        "pattern" 2      ; Token + pattern (e.g.,SORT mylist BY weight_*)
        "string" 2       ; Token + value (e.g., key value)
        "unix-time" 2    ; Token + value (e.g., PX 1732046457)
        1)))              ; Default to 1 for unknown types


(defn fetch-redis-commands []
  (let [result (shell {:out :string} "redis-cli --json COMMAND DOCS")]
    (if (= 0 (:exit result))
      (json/parse-string (:out result) keyword)
      (throw (Exception. "Failed to fetch Redis commands")))))

(defn flatten-arguments
  "Recursively flattens nested argument definitions into a single list.
   Preserves parent group information and parent type for child arguments."
  [args parent-group parent-type]
  (mapcat
   (fn [arg]
     (if-let [nested-args (:arguments arg)]
       ;; Recursively flatten nested arguments, inheriting group/type from parent
       (flatten-arguments nested-args
                          (or (:name arg) parent-group) ; Preserve group name
                          (or (:type arg) parent-type)) ; Preserve parent type
       ;; Base case: a single argument, add group/type info
       [(assoc arg :group parent-group :parent-type parent-type)]))
   args))

(defn argument-to-rule
  "Converts an argument definition to a parsing rule.
   Infers the size based on the argument's type."
  [{:keys [name type flags token group parent-type]}]
  (let [size (infer-size token type)]
    (cond-> {:size size
             :name name
             :type type
             :group group
             :parent-type parent-type
             :optional (some #{:optional} flags)}
      token (assoc :token token))))


(defn generate-option-rules
  "Generates parsing rules from Redis command documentation.
   Preserves group and parent-type information for downstream validation."
  [command-docs]
  (reduce
   (fn [rules-map [command {:keys [arguments]}]]
     ;; Flatten all arguments with group/type information, then generate rules
     (let [flat-args (flatten-arguments arguments nil nil)
           rules (map argument-to-rule flat-args)]
       (assoc rules-map command rules)))
   {}
   command-docs))


;; Example usage
(def command-docs
  {:pfmerge {:summary "Merges one or more HyperLogLog values into a single key."
             :since "2.8.9"
             :group "hyperloglog"
             :complexity "O(N) to merge N HyperLogLogs, but with high constant times."
             :arguments [{:name "destkey", :type "key", :display_text "destkey", :key_spec_index 0}
                         {:name "sourcekey", :type "key", :display_text "sourcekey", :key_spec_index 1, :flags ["optional" "multiple"]}]}})

(def ruleset (generate-option-rules (fetch-redis-commands)))

;; Save the ruleset to a file
(require '[clojure.edn :as edn])
(spit "redis-rules.edn" (pr-str ruleset))

