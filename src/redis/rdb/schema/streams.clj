(ns redis.rdb.schema.streams
  (:require
   [gloss.core :as gloss]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]
   [taoensso.timbre :as log]))


(defn parse-stream-id
  []
  (gloss/compile-frame
   (gloss/ordered-map :ms :uint64-be ;; High 64 bits of stream ID
                :seq :uint64-be ;; Low 64 bits of stream ID
                )))

;; Excellent treatise on listpacks
;; https://github.com/zpoint/Redis-Internals/blob/5.0/Object/listpack/listpack.md


;; The structure of the listpack is:
;; <tot-bytes> <num-elements> <element-1> ... <element-N> <listpack-end-byte>
;; Where each element is of the structure:
;; <encoding-type><element-data><element-tot-len>.
;; Reference: https://github.com/antirez/listpack/blob/master/listpack.md

(defn parse-stream-listpack
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :type :listpack
    :metadata :int16-le    ; 2 bytes of metadata
    :stream-id (parse-stream-id)  ; stream ID (ms + seq)
    :content (string/parse-string-encoded-value))))  ; raw listpack content as string

(defn parse-stream-listpacks2
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :type :listpack
    :metadata :int16-le    ; 2 bytes of metadata
    :stream-id (parse-stream-id)  ; stream ID (ms + seq)
    :content (string/parse-string-encoded-value))))  ; raw listpack content as string

;; ---------------------------------------------------------------------------- Layer 3
;; Depends only on Layer 2

(defn parse-stream-pel-entry
  [with-nacks?]
  (gloss/compile-frame
   (gloss/ordered-map
    :id (parse-stream-id)
    :nack (if with-nacks?
            (gloss/ordered-map
             :delivery-time (primitives/parse-length)  ; millisecond delivery time
             :delivery-count (primitives/parse-length)) ; delivery count
            (primitives/value-parser nil)))))  ; No NACK info for consumer PELs


;; ---------------------------------------------------------------------------- Layer 4
;; Depends only on Layer 3

(defn parse-stream-pel
  [with-nacks?]
  (gloss/repeated (parse-stream-pel-entry with-nacks?)
                  {:prefix (primitives/parse-length-prefix)}))

;; ---------------------------------------------------------------------------- Layer 6
;; Depends only on Layer 5`

(defn parse-stream-group-pel
  []
  (parse-stream-pel true))  ; Global PEL includes NACKs

(defn parse-consumer-pel
  []
  (parse-stream-pel false))  ; Consumer PEL excludes NACKs

;; ---------------------------------------------------------------------------- Layer 7
;; Depends only on Layer 6

(defn parse-stream-group-consumer
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :name (string/parse-string-encoded-value)
    :seen-time (primitives/parse-length)
    :active-time (primitives/parse-length)
    :pel (parse-consumer-pel))))

;; ---------------------------------------------------------------------------- Layer 8
;; Depends only on Layer 7

(defn parse-stream-group-consumer-list
  []
  (gloss/repeated 
   (parse-stream-group-consumer) 
   {:prefix (primitives/parse-length-prefix)}))

;; ---------------------------------------------------------------------------- Layer 9
;; Depends only on Layer 8

(defn parse-stream-group
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :name (string/parse-string-encoded-value)  ; Group name
    :last-id (parse-stream-id)  ; Last ID as ms+seq
    :entries-read (primitives/parse-length)  ; Number of entries read
    :pel (parse-stream-group-pel)  ; Global PEL with NACKs
    :consumers (parse-stream-group-consumer-list))))

;; ---------------------------------------------------------------------------- Layer 10
;; Depends only on Layer 9

(defn parse-stream-groups
  []
   (gloss/repeated
    (parse-stream-group)
    {:prefix (primitives/parse-length-prefix)}))

;; ---------------------------------------------------------------------------- Layer 11
;; Depends only on Layer 10

(defn parse-stream-listpack-3
  []
  (gloss/ordered-map
   :encoding :stream-listpack-v3
   :data (gloss/repeated (string/parse-string-encoded-value)
                         {:prefix (primitives/parse-length-prefix)})
   :elements-count (primitives/parse-length)
   :last-stream-id (parse-stream-id)
   :first-stream-id (parse-stream-id)
   :max-tombstone-id (parse-stream-id)
   :offset (primitives/parse-length)
   :groups (parse-stream-groups)))

   (defn parse-stream-listpack-3-bypass
   []
   (gloss/ordered-map :encoding :RDB_TYPE_STREAM_LISTPACKS_3
                      :metadata (gloss/compile-frame [:byte :byte])
                      :stream-id (parse-stream-id)
                      :content (string/parse-string-encoded-value)
                      :elements-count (primitives/parse-length-prefix)
                      :last-ms (primitives/parse-length)
                      :unknown (primitives/parse-length-prefix)
                      :first-ms (primitives/parse-length)
                      :unknown [:byte :byte :byte :byte :byte]))