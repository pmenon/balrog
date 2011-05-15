(ns balrog.queue
  (:use balrog.util
	balrog.ring-buffer
	balrog.subscription
	balrog.executor)
  (:require [clojure.contrib.logging :as logging]))

(set! *warn-on-reflection* true)

;(defprotocol Queue
;  (enqueue-msg [this msg])
;  (dequeue-msg [this msg])
;  (requeue-msg [this msg])
;  (requeue-all [this msgs])
;  (add-subscriber [this subscriber])
;  (remove-subscriber [this subscriber]))

;(defprotocol Backed
;  (store-msg [this msg])
;  (store-msgs [this msgs])
;  (retrieve-msg [this])
;  (retrieve-msgs [this num-msgs]))

;(deftype BalrogQueue [store msgs subscibers]
;  Backed
;  (store-msg [this msg]
;    (insert-msg this msg))
;  (store-msgs [this msgs]
;    (dorun (map (partial store-msg this) msgs)))
;  (retrieve-msg [this]
;    (get-message store 1))
;  (retrieve-msg [this num-msgs]
;    (get-message store num-msgs)))


(defn create-message [id msg]
  {:message-id id
   :message msg
   :create-ts (System/nanoTime)})

(defrecord Queue [name messages active-subs pending-subs dispatcher])

(def *msg-id-seq* (atom 1))

(defn next-msg-id []
  (swap! *msg-id-seq* inc))

(defn message-count [queue]
  (let [m @(:messages queue)
	rest (second m)]
    (count rest)))

(defn select-next-sub [queue]
  (let [subs (swap! (:active-subs queue) lrot)]
    (peek subs)))

(defn active-subscribers [queue]
  (count @(:active-subs queue)))

(defn pending-subscribers [queue]
  (count @(:pending-subs queue)))

(defn- deliver-msg [subscription message]
  (let [sub-fn (:sub-fn subscription)
	sub-id (:client-sub-id subscription)
	new-msg (-> message
		    (assoc :command :message)
		    (assoc-in [:headers :id] sub-id))]
    (logging/debug "Sending to subscription" (:sub-id subscription) "message" new-msg)
    (sub-fn new-msg)))

(defn- append-message [ [curr messages] message ]
    [curr (conj messages message)])

(defn- remove-message [ [curr messages] ]
  (if (== 0 (count messages))
    [:empty messages]
    [(peek messages) (pop messages)]))


;; (defn do-enqueue [[queue message]]
;;   (let [message (assoc-in message [:headers :message-id] (next-msg-id))]
;;     (if-let [recipient (select-next-sub queue)]
;;       (deliver-msg recipient message)
;;       (swap! (:messages queue) append-message message))))

;; (defn enqueue [queue message]
;;   ((:dispatcher queue) [queue message]))

(defn enqueue [queue message]
  (let [message (assoc-in message [:headers :message-id] (next-msg-id))]
    (if-let [recipient (select-next-sub queue)]
      (deliver-msg recipient message)
      (swap! (:messages queue) append-message message))))

(defn requeue [queue message]
  (enqueue queue message))

(defn requeue-all [queue messages]
  (doall (map #(enqueue queue %) messages)))

(defn dequeue [queue]
  (first (swap! (:messages queue) remove-message)))

(defn conj-or [subs subscriber]
  (let [sub-id (:sub-id subscriber)]
    (if (some #(= sub-id (:sub-id %)) subs)
      subs
      (conj subs subscriber))))

(defn add-subscriber [queue subscriber]
  (swap! (:active-subs queue) conj-or subscriber))

(defn- filter-out [subs sub-id]
  (reduce (fn [acc sub]
	    (if (= sub-id (:sub-id sub))
	      acc
	      (conj acc sub)))
	  clojure.lang.PersistentQueue/EMPTY
	  subs))

(defn remove-subscriber [queue sub-id]
  (swap! (:active-subs queue) filter-out sub-id))

(defn ack-message [queue sub-id message-id])

(defn nack-message [queue sub-id message-id])

(defn create-queue [name]
  (Queue. name
	  (atom [:empty clojure.lang.PersistentQueue/EMPTY])
	  (atom clojure.lang.PersistentQueue/EMPTY)
	  (atom {})
	  nil))


