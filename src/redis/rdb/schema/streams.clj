(ns redis.rdb.schema.streams
  (:require
   [gloss.core :as gloss]
   [manifold.stream :as ms]
   [redis.rdb.schema.bytes :as bytes]
   [redis.rdb.schema.primitives :as primitives]
   [redis.rdb.schema.string :as string]))


;; ---------------------------------------------------------------------------- Layer 0
;; Depends only on things outside this namespace

(defn parse-metadata-stream-id
  "Used to parse the stream ids in the stream metadata"
  []
  (gloss/compile-frame
   (gloss/ordered-map :ms (primitives/parse-length-prefix) ;; High 64 bits of stream ID
                      :seq (primitives/parse-length-prefix)))) ;; Low 64 bits of stream ID

(defn parse-stream-id 
  []
  (gloss/compile-frame  [:uint64-be :uint64-be]))


(defn parse-stream-data 
  []
  (gloss/header 
   (primitives/parse-length-prefix)
   (fn [size]
     (primitives/repeat-parser
      size
      (gloss/ordered-map
       :stream-id (string/parse-string-encoded-value)
       :listpack  (string/parse-string-encoded-value))))
   count))

;; ---------------------------------------------------------------------------- Layer 1
;; Depends only on Layer 1
(defn bytes->stream-id
  "Full stream ids are encoded as le-strings but the actual ID is read from the raw bytes."
  [bytes]
  @(-> bytes
       (bytes/get-byte-stream-parser (parse-stream-id))
       ms/take!))

(defn parse-stream-pel-entry
  [with-nacks?]
  (gloss/compile-frame
   (gloss/ordered-map
    :id (parse-metadata-stream-id)
    :nack (if with-nacks?
            (gloss/ordered-map
             :delivery-time (primitives/parse-length)  ; millisecond delivery time
             :delivery-count (primitives/parse-length)) ; delivery count
            (primitives/value-parser nil)))))  ; No NACK info for consumer PELs


;; ---------------------------------------------------------------------------- Layer 2
;; Depends only on Layer 1

(defn parse-stream-pel
  [with-nacks?]
  (gloss/repeated (parse-stream-pel-entry with-nacks?)
                  {:prefix (primitives/parse-length-prefix)}))

;; ---------------------------------------------------------------------------- Layer 3
;; Depends only on Layer 2

(defn parse-stream-group-pel
  []
  (parse-stream-pel true))  ; Global PEL includes NACKs

(defn parse-consumer-pel
  []
  (parse-stream-pel false))  ; Consumer PEL excludes NACKs

;; ---------------------------------------------------------------------------- Layer 4
;; Depends only on Layer 3

(defn parse-stream-group-consumer
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :name (string/parse-string-encoded-value)
    :seen-time (primitives/parse-length)
    :active-time (primitives/parse-length)
    :pel (parse-consumer-pel))))

;; ---------------------------------------------------------------------------- Layer 5
;; Depends only on Layer 4

(defn parse-stream-group-consumer-list
  []
  (gloss/repeated
   (parse-stream-group-consumer)
   {:prefix (primitives/parse-length-prefix)}))

;; ---------------------------------------------------------------------------- Layer 6
;; Depends only on Layer 5

(defn parse-stream-group
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :name (string/parse-string-encoded-value)  ; Group name
    :last-id (parse-metadata-stream-id)  ; Last ID as ms+seq
    :entries-read (primitives/parse-length)  ; Number of entries read
    :pel (parse-stream-group-pel)  ; Global PEL with NACKs
    :consumers (parse-stream-group-consumer-list))))

;; ---------------------------------------------------------------------------- Layer 7
;; Depends only on Layer 6

(defn parse-stream-groups
  []
  (gloss/repeated
   (parse-stream-group)
   {:prefix (primitives/parse-length-prefix)}))

;; ---------------------------------------------------------------------------- Layer 8
;; Depends only on Layer 7
 
;; placing here till we have a real parser for this version.
(defn parse-stream-listpacks
  []
  (gloss/compile-frame
   (gloss/ordered-map
    :type :listpack
    :metadata :int16-le    ; 2 bytes of metadata
    :stream-id (parse-metadata-stream-id)  ; stream ID (ms + seq)
    :content (string/parse-string-encoded-value))))  ; raw listpack content as string

(defn parse-stream-listpacks-2
  "Parses RDB_TYPE_STREAM_LISTPACKS_2"
  []
  (gloss/ordered-map
   :encoding :stream-listpack-v2
   :data (parse-stream-data)
   :element-count (primitives/parse-length-prefix)
   :last-stream-id (parse-metadata-stream-id)
   :first-stream-id (parse-metadata-stream-id)
   :max-tombstone-id (parse-metadata-stream-id)
   :offset (primitives/parse-length-prefix)
   :groups (parse-stream-groups)))

(defn parse-stream-listpacks-3
  "Despite being tagged as v3 in Redis 7.2.6, the data is actually written in v2 format.
   This parser handles that case until we need to support actual v3 format from newer Redis versions."
  []
  (parse-stream-listpacks-2))


;; ---------------------------------------------------------------------------- REPL

(comment

  ;; This is (close to) the actual parser for the v3 format
  (defn stream-listpack-v3-parser
    []
    (let [encoding :stream-listpack-v3
          listpacks (gloss/repeated
                     (gloss/ordered-map
                      :stream-id (parse-metadata-stream-id)
                      :listpack (string/parse-string-encoded-value))
                     {:prefix (primitives/parse-length-prefix)})
          elements-count (primitives/parse-length)
          last-stream-id (parse-metadata-stream-id)
          offset (primitives/parse-length)
          groups (parse-stream-groups)]
      (gloss/ordered-map
       :metadata {:encoding encoding
                  :elements-count elements-count
                  :last-stream-id last-stream-id
                  :offset offset
                  :groups groups}
       :data listpacks)))

  ;;  sample v2 parsers for debug in core.clj if needed
  (def stream-listpack-v2-parsers [(primitives/parse-header-byte)
                                   {:k (string/parse-string-encoded-value)}
                                   {:listpack-count (primitives/parse-length-prefix)}
                                   {:stream-id (string/parse-string-encoded-value)}
                                   {:listpack (string/parse-string-encoded-value)}
                                   {:elements-count (primitives/parse-length-prefix)}
                                   {:last-ms (primitives/parse-length)}
                                   {:last-seq (primitives/parse-length-prefix)}
                                   {:first-ms (primitives/parse-length)}
                                   {:first-seq (primitives/parse-length-prefix)}
                                   {:tombstone-ms (primitives/parse-length)}
                                   {:tombstone-seq (primitives/parse-length-prefix)}
                                   {:offset (primitives/parse-length)}
                                   {:groups (streams/parse-stream-groups)}])

  ;; bytes for a sample stream-listpack-v2
  ;; 68 70 63 C1 6D 02 FF 15 0B 72 61 63 65 3A 66 72
  ;; 61 6E 63 65 01 10 00 00 01 93 6F 51 8E 74 00 00
  ;; 00 00 00 00 C3 40 84 40 89 1F 89 00 00 00 00 00
  ;; 20 00 03 01 00 01 04 01 85 72 69 64 65 72 06 85
  ;; 73 70 65 65 64 06 88 70 6F 73 69 74 08 69 6F 6E
  ;; 09 8B 6C 6F 63 61 40 09 06 5F 69 64 0C 00 01 02
  ;; 20 2c 12 00 01 88 43 61 73 74 69 6C 6C 61 09 84
  ;; 33 30 2E 32 05 00 01 20 00 00 07 1b 13 F1 63 1C
  ;; 03 00 01 85 4e 6f 72 65 6D 06 84 32 38 2E 38 05
  ;; 03 C0 1A 01 8C 34 20 1A 10 88 50 72 69 63 6B 65
  ;; 74 74 08 84 32 39 2E 37 05 02 40 1D 01 FF

  [0x00 0x00 0x01 0x93 0x6F 0x51 0x8E 0x74  0x00 0x00 0x00 0x00 0x00 0x00]
  (def listpack-bytes
    [[0x01 0x10]
     [0x00 0x00 0x01 0x93 0x6F 0x51 0x8E 0x74 0x00 0x00 0x00 0x00 0x00 0x00] ;; stream id
     [0xC3 0x40 0x84 0x40 0x89 0x1F 0x89 0x00 0x00 0x00 0x00 0x00
      0x20 0x00 0x03 0x01 0x00 0x01 0x04 0x01 0x85 0x72 0x69 0x64 0x65 0x72 0x06 0x85
      0x73 0x70 0x65 0x65 0x64 0x06 0x88 0x70 0x6F 0x73 0x69 0x74 0x08 0x69 0x6F 0x6E
      0x09 0x8B 0x6C 0x6F 0x63 0x61 0x40 0x09 0x06 0x5F 0x69 0x64 0x0C 0x00 0x01 0x02
      0x20 0x2c 0x12 0x00 0x01 0x88 0x43 0x61 0x73 0x74 0x69 0x6C 0x6C 0x61 0x09 0x84
      0x33 0x30 0x2E 0x32 0x05 0x00 0x01 0x20 0x00 0x00 0x07 0x1b 0x13 0xF1 0x63 0x1C
      0x03 0x00 0x01 0x85 0x4e 0x6f 0x72 0x65 0x6D 0x06 0x84 0x32 0x38 0x2E 0x38 0x05
      0x03 0xC0 0x1A 0x01 0x8C 0x34 0x20 0x1A 0x10 0x88 0x50 0x72 0x69 0x63 0x6B 0x65
      0x74 0x74 0x08 0x84 0x32 0x39 0x2E 0x37 0x05 0x02 0x40 0x1D 0x01 0xFF] ;; lzh compressed listpack
     [0x03 0x81 0x00 0x00 0x01 0x93 0x6F 0x51 0xC3 0x00 ;; 2 bytes unknown + (:ms stream-id)
      0x00 0x81 0x00 0x00 0x01 0x93 0x6F 0x51 0x8E 0x74 ;; 2 bytes unknown + (:ms stream-id)
      0x00 0x00 0x00 0x3 0x00] ;; 5 unknown bytes
     ])
  )
