(ns balrog.stomp
  (:require [clojure.string :as str])
  (:import (java.io ByteArrayOutputStream)
	   (org.jboss.netty.buffer ChannelBuffer ChannelBuffers)))

(set! *warn-on-reflection* true)

(defrecord StompFrame [command headers body])

(def *line-end*  (byte 10))
(def *frame-end* (byte 0))
(def common-commands {"connect" :connect
		      "send" :send
		      "subscribe" :subscribe
		      "unsubscribe" :unsubscribe
		      "ack" :ack
		      "nack" :nack
		      "begin" :begin
		      "commit" :commit
		      "abort" :abort})

(let [keyword-cache (atom {})]
  (defn get-keyword [k]
    (if-let [entry (find @keyword-cache k)]
      (val entry)
      (let [new-cache (swap! keyword-cache assoc k (keyword k))
	    entry (find new-cache k)]
	(val entry)))))

(defn create-frame [command headers ^String body]
  (let [body-bytes (.getBytes body)]
    (if (keyword? command)
      (StompFrame. command headers body-bytes)
      (StompFrame. (get common-commands command) headers body-bytes))))

(defn error-frame [headers ^String reason]
  (StompFrame. :error headers (.getBytes reason)))

(defn nop-frame [] (StompFrame. :nil nil nil))

; Version 3 ! 45K/s
(defn ^String read-line-from-buffer [^ChannelBuffer buffer]
  (let [idx (.readerIndex buffer)]
    (loop [in-char (.readByte buffer) i 0]
      (if (= (int in-char) (int *line-end*))
	(String. (.. buffer toByteBuffer array) idx i)
	(recur (.readByte buffer) (inc i))))))

; Version 2 ~ 33K/s
;(defn ^String read-line-from-buffer [^ChannelBuffer buffer]
;  (.markReaderIndex buffer)
;  (loop [in-char (.readByte buffer) i 0]
;    (if (= (int in-char) (int *line-end*))
;      (let [barray (byte-array i)]
;	(.resetReaderIndex buffer)
;	(.readBytes buffer barray)
;	(.readByte buffer)
;	;(-> buffer (.resetReaderIndex) (.readBytes barray))
;	(String. barray))
;      (recur (.readByte buffer) (inc i)))))

; Version 1 ~ 22K/s
;(defn ^String read-line-from-buffer [^ChannelBuffer buffer]
;  (loop [in-char (.readByte buffer) buf (transient [])]
;    (if (= (int in-char) (int *line-end*))
;      (apply str (persistent! buf))
;      (recur (.readByte buffer) (conj! buf (char in-char))))))

(defn read-command [^ChannelBuffer buffer]
  (let [command (read-line-from-buffer buffer)]
    (get common-commands (.toLowerCase command))))

(defn write-command [^ChannelBuffer buffer command]
  (doto buffer
    (.writeBytes (.getBytes (.toUpperCase (name command))))
    (.writeByte *line-end*)))

(defn read-headers [^ChannelBuffer buffer]
  (loop [line (read-line-from-buffer buffer) headers (transient {}) ]
    (let [idx (.indexOf line ":")]
      (if (< idx 0)
	(persistent! headers)
	(let [^String key (.substring line 0 idx)
	      ^String val (.substring line (inc idx))]
	  (recur (read-line-from-buffer buffer)
		 (assoc! headers (get-keyword (.toLowerCase key)) val)))))))

;(defn read-headers [^ChannelBuffer buffer]
;  (loop [line (read-line-from-buffer buffer) headers (transient {}) ]
;    (let [key-val-pair (.split line ":")]
;      (if-not (= (alength key-val-pair) 2)
;	(persistent! headers)
;	(let [^String key (aget key-val-pair 0)
;	      ^String val (aget key-val-pair 1)]
;	  (recur (read-line-from-buffer buffer)
;		 (assoc! headers (get-keyword (.toLowerCase key)) val)))))))


;(defn write-headers [^ChannelBuffer buffer headers]
;  (for [[k v] headers]
;    (doto buffer
;      (.writeBytes (.getBytes ^String (name k)))
;      (.writeByte (int \:))
;      (.writeBytes (.getBytes ^String v))
;      (.writeByte *line-end*))))

(defn write-headers [^ChannelBuffer buffer headers]
  (let [lines (map (fn [[key val]]
		     (str (name key) ":" val "\n")) headers)]
    ; lines now represent a list of header values, eg "header:value"
    (reduce (fn [^ChannelBuffer buf ^String line]
    	      (.writeBytes buf (.getBytes line))
	      buf) buffer lines)))

(defn read-body [^ChannelBuffer buffer length]
  (if length
    (let [len (Integer/parseInt length)
	  body (byte-array len)]
      (if (> len (.readableBytes buffer))
	(throw (Exception. "Error: content-length of header is greater than available data"))
        (do
	  ;; read the body
	  (.readBytes buffer body)
	  ;; eat the terminating character
	  (.readByte buffer)
	  ;; return the body
	  body)))
    (loop [in-char (.readByte buffer) body (transient [])]
      (if (= (int in-char) (int *frame-end*))
	; when we reach the end, convert into a primitive byte[] because we don't need it ...
        (into-array Byte/TYPE (persistent! body))
	;; haven't reached frame end, loop back, read another byte and append to accumulator
        (recur (.readByte buffer) (conj! body in-char))))))

(defn write-body [^ChannelBuffer buffer ^bytes body]
  (.writeBytes buffer body))

(defn read-stomp-frame [^ChannelBuffer buffer]
  (let [command (read-command buffer)
	headers (read-headers buffer)
	body (read-body buffer (:content-length headers))]
    (StompFrame. command headers body)))

(defn write-stomp-frame [^StompFrame stomp-frame]
  (let [^ChannelBuffer buffer (ChannelBuffers/dynamicBuffer)]
    (doto buffer
      (write-command (:command stomp-frame))
      (write-headers (assoc (:headers stomp-frame) :content-length (count (:body stomp-frame))))
      (.writeByte *line-end*)
      (write-body (:body stomp-frame))
      (.writeByte *frame-end*))
    buffer))
