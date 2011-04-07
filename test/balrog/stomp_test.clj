(ns balrog.stomp-test
  (:use [balrog.stomp]
	[clojure.test] :reload)
  (:import (org.jboss.netty.buffer ChannelBuffers)))

(def connect-command "CONNECT\naccepted-version:1.0,1.1\ncontent-type:text/plain\n\ntest-string\0")
(def bad-connect-command "CONNECT\n\n\0")

(def connect-frame (balrog.stomp.StompFrame. :connect {:accepted-version "1.1" :key "value1"} (.getBytes "simple test")))

(defn create-buffer [frame]
  (ChannelBuffers/wrappedBuffer (.getBytes frame)))

(deftest stomp-can-read-command
  (let [buffer (create-buffer connect-command)
	command (read-command buffer)]
    (is (= :connect command))))

(deftest stomp-can-read-headers
  (let [buffer (create-buffer connect-command)
	command (read-command buffer)
	headers (read-headers buffer)]
    (is (= (count headers) 2))
    (is (= "1.0,1.1" (:accepted-version headers)))
    (is (= "text/plain" (:content-type headers)))))

(deftest stomp-can-parse-proper-frame
  (let [buffer (create-buffer connect-command)
	frame (read-stomp-frame buffer)
	headers (:headers frame)
	body (:body frame)]
    (is (= :connect (:command frame)))
    (is (= (count headers) 2))
    (is (.equals "1.0,1.1" (:accepted-version headers)))
    (is (.equals "text/plain" (:content-type headers)))
    (is (= nil (:fake-key headers)))
    (is (.equals "test-string" (String. body)))))

(deftest stomp-can-parse-improper-frames
  (let [buffer (create-buffer bad-connect-command)
	frame (read-stomp-frame buffer)
	headers (:headers frame)
	body (:body frame)]
    (is (= :connect (:command frame)))
    (is (empty? headers))))

(deftest stomp-can-write-command
  (let [buffer (ChannelBuffers/dynamicBuffer)]
    (write-command buffer :connect)
    (is (= (.readableBytes buffer) 8))
    (is (= (read-command buffer) :connect))))

(deftest stomp-can-write-headers
  (let [headers (:headers connect-frame)
	buffer (ChannelBuffers/dynamicBuffer)]
    (write-headers buffer headers)
    (.writeByte buffer 10)
    (let [headers (read-headers buffer)]
      (is (= (count headers) 2))
      (is (= (:accepted-version headers) "1.1"))
      (is (= (:key headers) "value1"))
      (is (= (:fake headers) nil)))))

(deftest stomp-can-encode-then-decode
  (let [buffer (write-stomp-frame connect-frame)
	frame (read-stomp-frame buffer)
	command (:command frame)
	headers (:headers frame)
	body (:body frame)]
      (is (= command :connect))
      (is (= (count headers) 3))
      (is (.equals "1.1" (:accepted-version headers)))
      (is (= nil (:fake headers)))
      (is (java.util.Arrays/equals (:body connect-frame) body))))