(ns balrog.core-test
  (:use balrog.core
	balrog.server
	balrog.test-utils
	clojure.test :reload))

(deftest balrog-on-connect-handles-proper-frames
  (let [broker (init-balrog)
	connection (atom (create-test-connection))
	frame (create-connect-frame)
	response (on-connect broker connection frame)]
    (is (= (:command response) :connected))
    (is (= (:connected? @connection) :connected))))

(deftest balrog-on-send-no-receipt-handles-proper-frames
  (let [broker (init-balrog)
	connection (atom (create-test-connection))
	frame (create-send-frame "/queue/a")
	response (on-send broker connection frame)]
    (is (= (:command response) :nil))))

(deftest balrog-on-send-with-receipt-properly-responds
  (let [broker (init-balrog)
	connection (atom (create-test-connection))
	frame (create-send-frame-with-receipt)
	response (on-send broker connection frame)]
    (is (= (:command response) :receipt))
    (is (= (get-in response [:headers :receipt-id]) "message-1234"))))

(deftest balrog-enables-subscription
  (let [broker (init-balrog)
	conn (atom (create-test-connection))]
    (testing "balrog properly subscribes with a good frame"
      (let [frame (create-subscribe-frame "/queue/test" 123)
	    response (on-subscribe broker conn frame)]
	(is (= (:command response) :nil))
	(is (= (count @(:queues broker)) 1))
	(let [sub-entry (get-in @conn [:subs 123])
	      destination (second sub-entry)]
	  (is (= (count (:subs @conn)) 1))
	  (is (= "/queue/test" destination)))))
    (testing "balrog gives an error on duplicate subscriptions"
      (let [frame (create-subscribe-frame "/queue/test" 123)
	    response (on-subscribe broker conn frame)]
	(is (= (:command response) :error))
	(is (= (count @(:queues broker)) 1))
	(is (= (count (:subs @conn)) 1))))))

(deftest balrog-enables-unsubscription
  (let [broker (init-balrog)
	conn (atom (create-test-connection))
	subscribe-frame (create-subscribe-frame "/queue/test" 123)
	unsubscribe-frame (create-unsubscribe-frame "/queue/test" 123)]
      (on-subscribe broker conn subscribe-frame)
      (is (= (count (:subs @conn)) 1))
      (let [response (on-unsubscribe broker conn unsubscribe-frame)]
	(is (= (:command response) :nil))
	(is (= (count (:subs @conn)) 0)))))

;; TODO: need to add tests for transactions
(deftest balrog-enables-acking-messages
  (testing "balrog properly acknowledges messages"
    (let [latch (java.util.concurrent.CountDownLatch. 1)
	  last-msg-id (atom 0)
	  broker (init-balrog)
	  conn (atom (create-test-connection (fn [msg]
					       (reset! last-msg-id (-> msg :headers :message-id))
					       (.countDown latch))))
	  subscribe-frame (create-ack-subscribe-frame "/queue/test" 999)
	  send-frame (create-send-frame "/queue/test")]
      ; subscribe to the queue
      (on-subscribe broker conn subscribe-frame)
      ; ensure the queue was created
      (is (= (count @(:queues broker)) 1))
      ; send the queue a message which gets dispatched to our queue asynchronously
      (on-send broker conn send-frame)
      ; await for the message
      (.await latch)
      (let [pending-acks-before (get-in @conn [:pending-acks 999])
	    ack-frame (create-acknowledge-frame "/queue/test" 999 @last-msg-id)
	    response (on-ack broker conn ack-frame)
	    pending-acks-after (get-in @conn [:pending-acks 999])]
	(is (= (count pending-acks-before) 1))
	(is (= (:command response) :nil))
	(is (= (count pending-acks-after) 0))))))

;(testing "balrog properly acknowledges batches of message"
;      ;; enqueue 3 messages, all should be sent to the single subscription
;      (on-send broker conn send-frame)
;      (on-send broker conn send-frame)
;      (on-send broker conn send-frame)
;      (let [pending-acks (get-in @conn [:pending-acks 999])
;	    ack-message-id (get-in (second pending-acks) [:headers :message-id])
;	    ack-frame (create-acknowledge-frame "/queue/test" 999 ack-message-id)
;	    response (on-ack broker conn ack-frame)
;	    pending-acks-after (get-in @conn [:pending-acks 999])]
;	(println pending-acks)
;	(is (= (count pending-acks) 3))
;	(is (= (:command response) :nil))
;	(is (= (count pending-acks-after) 1))))))

;; TODO: need to add tests for transactions
(deftest balrog-enables-nacking-messages
  (testing "balrog properly nack single messages"
    (let [latch (java.util.concurrent.CountDownLatch. 1)
	  last-msg-id (atom 0)
	  broker (init-balrog)
	  conn (atom (create-test-connection (fn [msg]
					       (reset! last-msg-id (-> msg :headers :message-id))
					       (.countDown latch))))
	  subscribe-frame (create-ack-subscribe-frame "/queue/test" 999)
	  unsubscribe-frame (create-unsubscribe-frame "/queue/test" 999)
	  send-frame (create-send-frame "/queue/test")]
      ; subscribe to the queue
      (on-subscribe broker conn subscribe-frame)
      ; ensure queue is created
      (is (= (count @(:queues broker)) 1))
      ; send a message to the queue, which dispatches to single acking subscriber asynchronously
      (on-send broker conn send-frame)
      ; wait till the message is sent
      (.await latch)
      ; ensure it's queued up in out pending-acks queue
      (is (= (count (get-in @conn [:pending-acks 999])) 1))
      ; TODO: the below behanviour needs to change!
      ; unsubscribe so the nack won't requeue with us
      (on-unsubscribe broker conn unsubscribe-frame)
      (let [pending-acks-before (get-in @conn [:pending-acks 999])
	    ack-message-id (get-in (first pending-acks) [:headers :message-id])
	    nack-frame (create-nack-frame "/queue/test" 999 ack-message-id)
	    response (on-nack broker conn nack-frame)
	    pending-acks-after (get-in @conn [:pending-acks 999])]
	(is (= (count pending-acks-before) 1))
	(is (= (:command response) :nil))
	(is (= (count pending-acks-after) 0))))))

;(testing "balrog properly nack's multiple messages"
;    (let [broker (init-balrog)
;	  conn (atom (create-test-connection))
;	  subscribe-frame (create-ack-subscribe-frame "/queue/test" 555)
;	  unsubscribe-frame (create-unsubscribe-frame "/queue/test" 555)
;	  send-frame (create-send-frame "/queue/test")]
      ;; subscribe to the queue
;      (on-subscribe broker conn subscribe-frame)
;      ;; send three messages to the queue to out subscriber
;      (on-send broker conn send-frame)
;      (on-send broker conn send-frame)
;      (on-send broker conn send-frame)
      ;; check we've got em
;      (is (= (count (get-in @conn [:pending-acks 555])) 3))
;      (on-unsubscribe broker conn unsubscribe-frame)
;      (let [pending-acks (get-in @conn [:pending-acks 555])
;	    ack-message-id (get-in (second pending-acks) [:headers :message-id])
;	    nack-frame (create-nack-frame "/queue/test" 555 ack-message-id)
;	    response (on-nack broker conn nack-frame)
;	    pending-acks-after (get-in @conn [:pending-acks 555])]
;	(is (= (count pending-acks) 3))
;	(is (= (:command response) :nil))
;	(is (= (count pending-acks-after) 1))))))
