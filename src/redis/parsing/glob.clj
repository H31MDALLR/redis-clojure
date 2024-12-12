(ns redis.parsing.glob
  (:require
   [clojure.string :as str]
   [instaparse.core :as insta]
   [taoensso.timbre :as log]))

(def glob-grammar
  (insta/parser
   "
(* Entry point of the grammar *)
    
(* High-Level Constructs *)

(* A glob-word is zero or more glob-elements *)
glob-word = glob-element*

(* Glob-elements can be a bracketed pattern, a curly group, or a glob-char *)
<glob-element> = glob-bracket-pos 
    | glob-bracket-neg
    | glob-curly-group
    | glob-char

(* Curly group: '{' glob-word (',' glob-word)* '}' *)
glob-curly-group = <'{'> glob-word (<','> glob-word)* <'}'>

(* Bracketed patterns *)
glob-bracket-pos = <'['> bracket-chars <']'>
glob-bracket-neg = <('[!' | '[^')> bracket-chars <']'>

(* bracket-chars: zero or more characters that are not ']' *)
<bracket-chars> = #\"[^]]*\"

(* Glob chars *)
(* glob-literal-char should exclude special glob chars: [ ] { } * ? , 
   so that these are handled by glob-bracket, glob-curly-group, and glob-match rules *)
<glob-char> = glob-literal
          | glob-match-many
          | glob-match-one

(* glob-literal-char is any printable char except the special glob chars *)
(* Special glob characters:    '[' ']' '{' '}' '*' '?' ',' 
   We exclude these from glob-literal-char. *)

glob-literal = glob-literal-char+
<glob-literal-char> = regular-chars
<regular-chars> = regular-char+
<regular-char> = #\"[^\\[\\]\\{\\}\\*\\?\\,]\"

glob-match-many = '*'
glob-match-one = '?'

(* Quoted literals *)
quoted-literal = single-quote | double-quote
single-quote = \"'\" literal* \"'\"
double-quote = '\"' literal* '\"'

(* Identifiers *)
identifier = identifier-prefix identifier-suffix*

identifier-prefix = alphabetic | '_'
identifier-suffix = alphanumeric | '_'

alphabetic = upper-case | lower-case
alphanumeric = alphabetic | digit

digit = #\"[0-9]\"
upper-case = #\"[A-Z]\"
lower-case = #\"[a-z]\"

(* Literals (used inside quotes) *)
(* literal = printable* is still allowed for inside quotes,
   but since 'word' no longer defaults to literal, 
   it won't overshadow glob matches. *)
literal = printable*

(* 'printable' as all ASCII printable characters except commas and newlines (0x20 to 0x7E) *)
printable = #\"[ -~&&[^,]]\"
"
   :auto-whitespace :comma))

(defn escape-regex-chars 
  [s]
  (str/replace s #"([\.\^\$\+\?\{\}\(\)\|\[\]\\])" "\\\\$1"))

(def to-regex-transform
  "Transform glob AST to a regex string"
  (partial insta/transform
           {; A glob word is a sequence of elements concatenated together
            :glob-word        (fn [& elements] (apply str elements))

            ; Glob literals (sequences of regular chars) are escaped to avoid regex metacharacter issues
            :glob-literal     (fn [& chars] (->> chars (apply str) escape-regex-chars))

            ; Glob match many: '*' => `[^/]*`
            :glob-match-many  (fn [_] "[^/]*")

            ; Glob match one: '?' => `[^/]`
            :glob-match-one   (fn [_] "[^/]")

            ; Bracketed sets remain mostly the same, no escape since they form a character class
            ; e.g. '[a-z]' => '[a-z]', '[!abc]' => '[^abc]'
            :glob-bracket-pos (fn [content] (str "[" content "]"))
            :glob-bracket-neg (fn [content] (str "[^" content "]"))

            ; Curly groups: '{foo,bar}' => '(?:foo|bar)'
            ; Each glob-word inside is transformed to its regex form; we join them with '|'
            :glob-curly-group (fn [& patterns] (str "(?:" (str/join "|" patterns) ")"))

            ; Identifiers, quoted literals, and ordinary literals also get escaped
            :identifier       (fn [& chars] (->> chars (apply str) escape-regex-chars))
            :quoted-literal   (fn [& chars] (->> chars (apply str) escape-regex-chars))
            :literal          (fn [& chars] (->> chars (apply str) escape-regex-chars))
            :single-quote     (fn [& chars] (->> chars (apply str) escape-regex-chars))
            :double-quote     (fn [& chars] (->> chars (apply str) escape-regex-chars))}))

(defn glob->regex
  "Transforms a glob pattern into a regular expression string"
  [glob]
  (letfn [(convert [ast]
            (case (:tag ast)
              :pattern (apply str (map convert (:content ast)))
              :wildcard (case (first (:content ast))
                          "*" ".*"
                          "?" ".")
              :char-group (let [[neg? & ranges] (:content ast)]
                            (str "["
                                 (if (= neg? "!") "^" "")
                                 (apply str ranges)
                                 "]"))
              :alternation (str "(" (apply str (interpose "|" (map convert (:content ast)))) ")")
              :character (first (:content ast))
              :altexpr (first (:content ast))
              (throw (ex-info "Unknown tag" {:ast ast}))))]
    (->> (glob-grammar glob)
         (insta/transform {:segment identity})
         convert)))

(defn glob-matches? 
  [regex-str key] 
  (let [regex (re-pattern regex-str)]
    (if (string? key)
      (re-matches regex key)
      (do
        (log/error ::glob-matches? {:anomaly :anomalies/incorrect
                                    :regex   regex-str
                                    :key     key})
        nil))))


(defn match-keys [glob-pattern keys]
  (log/trace ::match-keys {:glob-patter glob-pattern
                           :keys keys})
  (let [regex-str  (-> glob-pattern glob-grammar to-regex-transform)]
    (log/trace ::match-keys {:keys keys
                             :regex regex-str})
    (filter #(if (string? %)
               (glob-matches? regex-str %)
               (do
                 (log/error ::match-keys {:anomaly :anomalies/incorrect :glob-pattern glob-pattern :key %})
                 false))
            keys)))


;; ---------------------------------------------------------------------------- REPL Area

(comment

;; -------------------------------------------------------- parser test
  
  (do
    (log/set-min-level! :trace)
    (def transformer
      (partial insta/transform
               {:glob-literal str}))

    (def glob-patterns ["[!u,s,r]"
                        "[a-z]er:123"
                        "user:*"
                        "o?der*"
                        "{*.123, *.xyz}"
                        "*-{[0-9],draft}.docx"])
    (try
      (map #(-> % glob-grammar to-regex-transform) glob-patterns)
      (catch Throwable t
        (log/error t))))

  ;; -------------------------------------------------------- Parser -> Regex E2E
  

  (let [examples [["[!u,s,r]*" ["user:123", "user:abc", "order:123", "user:xyz"] ["order:123"]]
                  ["[a-z]?er:123" ["user:123", "user:abc", "order:123", "user:xyz"] ["user:123"]]
                  ["[a-z][!s]?er:123" ["user:123", "user:abc", "order:123", "user:xyz"] ["order:123"]]
                  ["user:*" ["user:123", "user:abc", "order:123", "user:xyz"] ["user:123" "user:abc" "user:xyz"]]
                  ["o?der*" ["user:123", "user:abc", "order:123", "user:xyz"] ["order:123"]]
                  ["{*:123, *:xyz}"  ["user:123", "user:abc", "order:123", "user:xyz"] ["user:123" "order:123" "user:xyz"]]]]
    (map (fn [[glob-pattern keys expected]]
           (let [regex (-> glob-pattern glob-grammar to-regex-transform)]
             (try
               (let [results (->> keys (map #(glob-matches? regex %)) (filter seq))]
                 {:results results
                  :pass?   (= results expected)})
               (catch clojure.lang.ExceptionInfo e
                 (log/error ::repl e)
                 {:pattern glob-pattern
                  :ex-data (ex-data e)}))))
         examples))

    (let [examples [["[!u,s,r]*" ["user:123", "user:abc", "order:123", "user:xyz"] ["order:123"]]
                  ["[a-z]?er:123" ["user:123", "user:abc", "order:123", "user:xyz"] ["user:123"]]
                  ["[a-z][!s]?er:123" ["user:123", "user:abc", "order:123", "user:xyz"] ["order:123"]]
                  ["user:*" ["user:123", "user:abc", "order:123", "user:xyz"] ["user:123" "user:abc" "user:xyz"]]
                  ["o?der*" ["user:123", "user:abc", "order:123", "user:xyz"] ["order:123"]]
                  ["{*:123, *:xyz}"  ["user:123", "user:abc", "order:123", "user:xyz"] ["user:123" "order:123" "user:xyz"]]]]
    (map (fn [[glob-pattern keys expected]]
             (try
               (log/trace ::match-keys-test {:pattern glob-pattern
                                             :keys keys})
               (let [results (->> keys (match-keys glob-pattern))]
                 {:results results
                  :pass?   (= results expected)})
               (catch clojure.lang.ExceptionInfo e
                 (log/error ::repl e)
                 {:pattern glob-pattern
                  :ex-data (ex-data e)})))
         examples))
    
    ;; -------------------------------------------------------- random test area
  
  (re-matches #"[^u,s,r]*" "order:123")
  (re-matches #"[a-z][a-z]er:123" "order:123")
  (re-matches #"[^u,s,r][^/]*" "order:123")

  "Leave this here."
  )
