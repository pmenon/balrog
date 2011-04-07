(ns balrog.test-utils
  (:use balrog.stomp
	balrog.server)
  (:import (org.jboss.netty.channel Channel)))

(defn mock-channel
  ([]
     (reify Channel
       (write [this msg])))
  ([f]
     (reify Channel
       (write [this msg]
	 (f msg)))))

(defn create-test-connection
  ([]
     (create-connection :not-connected (mock-channel)))
  ([f]
     (create-connection :not-connected (mock-channel f))))

(defn stomp-frame [type headers body]
  (balrog.stomp.StompFrame. type headers (.getBytes body)))


;; CONNECT
(defn create-connect-frame [] (stomp-frame :connect {:accept-version "1.1"} ""))

;; SEND
(defn create-send-frame [q-name]
  (stomp-frame :send {:destination q-name} "test string"))
(defn create-send-frame-with-receipt []
  (stomp-frame :send {:destination "/queue/a" :receipt "message-1234"} "test string"))

;; SUBSCRIBE
(defn create-subscribe-frame [q-name id]
  (stomp-frame :subscribe {:destination q-name :id id} ""))
(defn create-ack-subscribe-frame [q-name id]
  (stomp-frame :subscribe {:destination q-name :id id :ack :client} ""))

;; UNSUBSCRIBE
(defn create-unsubscribe-frame [q-name id]
  (stomp-frame :unsubscribe {:destintation q-name :id id} ""))

;; ACKNOWLEDGE
(defn create-acknowledge-frame [q-name id message-id]
  (stomp-frame :ack {:destination q-name :id id :message-id message-id} ""))

;; NOT-ACKNOWLEDGE
(defn create-nack-frame [q-name id message-id]
  (stomp-frame :nack {:destination q-name  :id id :message-id message-id} ""))
;; Bad Connect
(defn create-bad-connect-frame [] (stomp-frame :connect {:accept-version "0.1"} ""))

;; Bad Send
(defn create-bad-send-frame [] (stomp-frame :send {} "test string"))

;; Bad Subscribe
(defn create-incomplete-subscribe-frame [] (stomp-frame :subscribe {:destination "/queue/test"} ""))
(defn create-no-destination-subscribe-frame [] (stomp-frame :subscribe {:id 123} ""))

;; Bad Unsubscribe
(defn create-unsubscribe-frame-missing-id [] (stomp-frame :unsubscribe {} ""))

;; Bad Ack 
(defn create-acknowledge-frame-no-id [q-name message-id]
  (stomp-frame :ack {:destination q-name :message-id message-id} ""))

(defn create-acknowledge-frame-no-message-id [q-name id]
  (stomp-frame :ack {:destination q-name :id id} ""))

;; Bad Nack
(defn create-nack-frame-no-id [q-name message-id]
  (stomp-frame :nack {:destination q-name :message-id message-id} ""))

(defn create-nack-frame-no-message-id [q-name id]
  (stomp-frame :nack {:destination q-name :id id} ""))


