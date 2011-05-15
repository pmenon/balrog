(ns balrog.core
  (:use balrog.queue
	balrog.executor
        balrog.server
	balrog.stomp
	balrog.subscription
	balrog.util
	balrog.validation)
  (:require [clojure.contrib.logging :as logging]
	    [clojure.contrib.core :as core])
  (:import (org.jboss.netty.channel Channel)
	   (org.fusesource.hawtdispatch DispatchSource EventAggregators))
  (:gen-class))

(set! *warn-on-reflection* true)

;; TODO: Each of the handler functions need not be aware that
;;       the connection is a reference.  We need to stop treating it as such!

;; TODO: Add more dang comments!

;; The port we're going to listen on
(def *balrog-port* 61613)

(declare handle-frame)

;; This is going to become more complicated!
(defn init-balrog []
  {:supported-versions #{1.1 1.0}
   :welcome-message "You're hooked up with Balrog, baby!"
   :queues (atom {})})

(defn is-supported? [versions client-version]
  (let [client-versions (.split client-version ",")]
    (reduce #(or %1 (contains? versions (Double/parseDouble %2))) false client-versions)))

(defn get-or-create-queue! [broker q-name]
  (let [queues (:queues broker)
	new-queues (swap! queues update-with q-name (fn [_] (create-queue q-name)))]
    (get new-queues q-name)))

(defn on-connect [broker connection stomp-message]
  (let [client-version (-> stomp-message :headers :accept-version)
	supported-versions (:supported-versions broker)]
    (if (and client-version (not (is-supported? supported-versions client-version)))
      (error-frame {:version (reduce #(str %1 "," %2) supported-versions)}
		   "Get your version game up, son!")
      (do
	(swap! connection assoc :connected? :connected)
	(create-frame :connected {:version "1.1" :session-id (generateUUID)}
		      (:welcome-message broker))))))

(defn on-send [broker connection stomp-message]
  (let [destination (-> stomp-message :headers :destination)]
    (if (not destination)
      (error-frame {} "Error: Destination is required for send frames.")
      (let [receipt (-> stomp-message :headers :receipt)
	    queue (get-or-create-queue! broker destination)]
	;; Enqueue the message!
        (enqueue queue stomp-message)
	(if receipt
	  (create-frame :receipt {:receipt-id receipt} "Your message was sent, son!")
	  (nop-frame))))))

(defn on-subscribe [broker connection stomp-message]
  (let [destination (-> stomp-message :headers :destination)
	id (-> stomp-message :headers :id)
	ack (-> stomp-message :headers :ack)
	existing-sub? (contains? (:subs @connection) id)]
    (cond
     (or (not destination) (not id))
     (error-frame {} "Error: Destination and ID are required for subscibe frames.")

     existing-sub?
     (error-frame {} "Error: Subscription with that ID already exists on this connection.")

     :else
     (let [queue (get-or-create-queue! broker destination)
	   callback-fn (fn [msg]
			 (when (or (= :client ack) (= :client-individual ack))
			   (swap! connection add-ack id msg))
			 (.write ^Channel (:channel @connection) msg))
	   sub (create-subscription (generateUUID) id ack callback-fn)]
       (swap! connection add-subscription sub destination)
       (add-subscriber queue sub)
       (nop-frame)))))


(defn on-unsubscribe [broker connection stomp-message]
  (let [id (-> stomp-message :headers :id)
	[sub destination] (get-in @connection [:subs id])
	sub-id (:sub-id sub)]
    (cond
     (not id) (error-frame {} "Son, you must specify the subscription ID to unsubscribe!")
     (not sub-id) (error-frame {} "Son, don't play games! This subscription ID don't exist!")
     (not destination) (error-frame {} "Weird, for some reason the queue has disappeared :S")
     :else
     (let [queue (get @(:queues broker) destination)]
       (remove-subscriber queue sub-id)
       (swap! connection remove-subscription id)
       (nop-frame))))) 

(defn- do-ack [connection sub message-id]
  (let [sub-id (:client-sub-id sub)
	pending-acks (get-in connection [:pending-acks sub-id])
	[head tail] (split-with #(not (= message-id (-> % :headers :message-id))) pending-acks)]
    (when-not (= (count head) (count pending-acks))
      (if (= :client (:ack sub))
	(set-acks connection sub-id (rest tail))
	(set-acks connection sub-id (concat head (rest tail)))))))

(defn- do-nack [connection sub message-id]
  (let [sub-id (:client-sub-id sub)
	pending-acks (get-in @connection [:pending-acks sub-id])
	[head tail] (split-with #(not (= message-id (-> % :headers :message-id))) pending-acks)]
    (when-not (= (count head) (count pending-acks))
      (if (= :client (:ack sub))
	[head (connection set-acks sub-id (rest tail))]
	[(first second) (set-acks connection sub-id (concat head (rest second)))]))))

(defn on-ack [broker connection stomp-message]
  (let [message-id (-> stomp-message :headers :message-id)
	client-sub-id (-> stomp-message :headers :id)
	client-txn-id (-> stomp-message :headers :transaction)]
    (cond
     (not message-id) (error-frame {} "Error: Message ID is required for ACK frames")
     (not client-sub-id) (error-frame {} "Error: ID is required for ACK frames")
     
     client-txn-id
     (if-let [txns (get-in @connection [:txns client-txn-id])]
       (do
	 (swap! (:txns broker) add-to-txn client-txn-id stomp-message)
	 (nop-frame))
       (error-frame {} "Error: The provided transaction does not exist"))

     :else
     (let [[sub destination] (get-in @connection [:subs client-sub-id])]	   
       (swap! connection do-ack sub message-id)
       (nop-frame)))))

(defn on-nack [broker connection stomp-message]
  (let [message-id (-> stomp-message :headers :message-id)
	client-sub-id (-> stomp-message :headers :id)
	client-txn-id (-> stomp-message :headers :transaction)]
    (cond
     (not message-id) (error-frame {} "Error: Message ID is required for NACK frames")
     (not client-sub-id) (error-frame {} "Error: ID is required for NACK frames")
     
     client-txn-id
     (if-let [txn-id (get-in @connection [:txns client-txn-id])]
       (do
	 (swap! (:txns broker) update-with txn-id #(conj % stomp-message))
	 (nop-frame))
       (error-frame {} "Error: The provided transaction does not exist"))

     :else
     (let [[sub destination] (get-in @connection [:subs client-sub-id])
	   queue (get @(:queues broker) destination)
	   [requeue-messages new-conn] (do-nack connection sub message-id)]
       (reset! connection new-conn)
       (requeue-all queue requeue-messages)
       (nop-frame)))))

(defn on-begin [broker connection stomp-message]
  (let [txn-id (-> stomp-message :headers :trasaction)]
    (swap! connection begin-txn txn-id)
    (nop-frame)))

(defn on-commit [broker connection stomp-message]
  (let [txn-id (-> stomp-message :headers :transaction)
        txn-messages (get-in @connection [:txns txn-id])]
    (map #(handle-frame broker
			connection
			(core/dissoc-in % [:headers :transaction])) txn-messages)))

(defn on-abort [broker connection stomp-message]
  (let [txn-id (-> stomp-message :headers :transaction)]
    (swap! connection abort-txn txn-id)
    (nop-frame)))

(defn handle-frame [broker connection stomp-message]
  (let [command (:command stomp-message)]
    [connection
    (cond
     (= :connect command) (on-connect broker connection stomp-message)
     (= :send command) (on-send broker connection stomp-message)
     (= :subscribe command) (on-subscribe broker connection stomp-message)
     (= :unsubscribe command) (on-unsubscribe broker connection stomp-message)
     (= :ack command) (on-ack broker connection stomp-message)
     (= :nack command) (on-nack broker connection stomp-message)
     (= :begin command) (on-begin broker connection stomp-message)
     (= :commit command) (on-commit broker connection stomp-message)
     (= :abort command) (on-abort broker connection stomp-message)
     :else (error-frame {} "Error: Unknown STOMP command received.  Dropping frame ..."))]))

(defn wrap-validation [next broker]
  (fn [conn frame]
    (let [[new-conn new-frame] (validate broker @conn frame)]
      (if-not (= frame new-frame)
	[conn new-frame]
	(next conn frame)))))

(defn wrap-logging [next]
  (fn [conn frame]
    (logging/debug "Received frame: " frame)
    (next conn frame)))

(defn wrap-security [next broker auth-fn]
  (fn [conn frame]
    (if-not (auth-fn broker conn frame)
      [conn (error-frame {} "Error: Connection is not allowed access to this operation")]
      (next conn frame))))

(defn create-request-handler []
  (let [broker (init-balrog)]
    (-> (partial handle-frame broker)
	(wrap-logging)
	(wrap-validation broker)
	(wrap-security broker (fn [_ _ _] true)))))

(defn -main [& args]
  (let [request-handler (create-request-handler)]
    (logging/info "Starting BalrogQ Stomp server on port 61613 ...")
    (create-balrog-server request-handler *balrog-port*)))
