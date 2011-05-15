(ns balrog.server
  (:use [balrog.netty :as netty]
	balrog.stomp)
  (:import (org.jboss.netty.channel Channel))
  (:require [clojure.contrib.logging :as logging]))

(set! *warn-on-reflection* true)

(defrecord StompConnection [connected? channel subs txns pending-acks])

(defn create-connection [connected? channel]
  (StompConnection. connected? channel {} {} {}))

(defn add-ack [conn sub-id acking-message]
  (update-in conn [:pending-acks sub-id] #(conj % acking-message)))

(defn set-acks [conn sub-id acks]
  (assoc-in conn [:pending-acks sub-id] acks))

(defn find-pending-msg [conn msg-id]
  (filter #(= msg-id (-> % :headers :message-id)) (:pending-acks conn)))

(defn add-subscription [conn sub destination]
  (update-in conn [:subs] #(assoc % (:client-sub-id sub) [sub destination])))

(defn remove-subscription [conn sub-id]
  (update-in conn [:subs] #(dissoc % sub-id)))

(defn begin-txn [conn txn-id]
  (assoc-in conn [:txns txn-id] []))

(defn abort-txn [conn txn-id]
  (update-in conn [:txns] #(dissoc % txn-id)))

(defn add-to-txn [conn txn-id msg]
  (update-in conn [:txns txn-id] #(conj % msg)))

(defn remove-txn [conn txn-id]
  (update-in conn [:txns] #(dissoc % txn-id)))

(defn clean-up [conn])

(defprotocol Handler
  (connect [this ctx event])
  (message [this ctx event])
  (exception [this ctx event])
  (disconnected [this ctx event]))

(defn dispatch-msg [state msg]
  (let [handler (:handler state)
	conn (:conn state)
	[new-conn response] (handler conn msg)]
    (when-not (=  :nil (:command response))
      (.write ^Channel (:channel @conn) response))
    state))

(defn delegating-handler [handler]
  (let [conn (atom (create-connection :not-connected nil))
	dispatcher (agent {:handler handler :conn conn})]
    (def-netty-handler
      (connect [ctx event]
        (swap! conn assoc :channel (.getChannel event)))
      (message [ctx event]
        (send dispatcher dispatch-msg (.getMessage event)))
      (exception [ctx event]
        (logging/error (.getCause event) "ERROR")
	(clean-up (.getChannel event))
	(.. event getChannel close))
      (disconnected [ctx event]
        (logging/debug "DISCONNECTED")
	(clean-up (.getChannel event))))))

;; Our pipeline
(defn create-balrog-pipeline [handler]
  (netty/pipeline
   "balrog-stomp-encoder" (encoder write-stomp-frame)
   "balrog-delimeter-decoder" (delimeter-based-decoder)
   "balrog-stompdecoder" (decoder read-stomp-frame)
   "balrog-handler" (delegating-handler handler)))

;; Create out server
(defn create-balrog-server [handler port]
  (create-nio-server #(create-balrog-pipeline handler) port))

