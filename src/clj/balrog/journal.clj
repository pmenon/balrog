(ns balrog.journal
  (:use [balrog.stomp])
  (:import (java.nio ByteBuffer)
	   (java.nio.channels FileChannel)
	   (java.io File FileInputStream FileOutputStream)
	   (org.jboss.netty.buffer ChannelBuffers)))

(def *entry-size-len* 4)

; A protocol for journal entries
(defprotocol JournalEntry
  (size [this])
  (^ByteBuffer pack [this]))

(extend-protocol JournalEntry
  balrog.stomp.StompFrame
  (size [this] );(frame-size this))
  (pack [this]
    (let [ch-buffer (write-stomp-frame this)]
      (.toByteBuffer ch-buffer))))

(defn unpack [bb]
  (let [ch-buffer (ChannelBuffers/wrappedBuffer bb)]
    (read-stomp-frame ch-buffer)))

(defn read-from-channel [fc n]
  (let [tbuf (ByteBuffer/allocate n)]
    (.read fc tbuf)
    (.rewind tbuf)))

(defprotocol Journal
  (can-read [this])
  (add-entry [this entry])
  (read-entry [this])
  (flush-out [this f]))

(deftype FileJournal [^FileChannel reader ^FileChannel writer]
  Journal
  (can-read [this]
    (< (.position reader) (.position writer)))
  (add-entry [this entry]
    (let [buffer (pack entry)
	  tbuf (ByteBuffer/allocate 4)]
      (.write writer (.. tbuf (putInt (.remaining buffer)) rewind))
      (.write writer buffer)
      (.force writer false)))
  (read-entry [this]
    (when (can-read this)
      (let [tbuf (read-from-channel reader *entry-size-len*)
	    output (read-from-channel reader (.getInt tbuf))]
	(unpack output))))
  (flush-out [this f]
    (loop [entry (read-entry this)]
      (when entry
	(f entry)
	(recur (read-entry this))))))

(def *root* "/Users/menonp/development/balrog/data/")
(defn create-file-journal [qname]
  (let [writer (-> (File. (str *root* qname ".data")) (FileOutputStream. true) .getChannel)
	reader (-> (File. (str *root* qname ".data")) (FileInputStream.) .getChannel)]	
    (FileJournal. reader writer)))