(ns balrog.queue-test
  (:use [balrog.queue]
	[balrog.subscription]
	[balrog.util]
	[clojure.test] :reload))

(deftest can-create-a-message
  (let [message (create-message 1 "test")]
    (is (= "test" (:message message)))))

(deftest can-create-a-queue
  (let [queue (create-queue "queue1")]
    (is (= "queue1" (:name queue)))))

(deftest can-enqueue-message-to-queue
  (let [message (create-message 1 "test message")
	queue (create-queue "queue1")]
    (enqueue queue message)
    (is (= 1 (message-count queue)))))

(deftest can-dequeue-message-from-queue
  (let [message (create-message 1 "test message")
	queue (create-queue "queue1")]
    ;; enqueue a message
    (enqueue queue message)
    (is (= 1 (message-count queue)))
    (is (= "test message" (:message (dequeue queue))))
    (is (= 0 (message-count queue)))))

(deftest queues-can-handle-subscribers
  (let [queue (create-queue "queue1")
	subscriber (create-subscription 1 1 :auto nil)
	subscriber2 (create-subscription 2 1 :auto nil)]
    ;; add a subcriber
    (testing "queues can add subscribers"
      (add-subscriber queue subscriber)
      (is (= 1 (active-subscribers queue))))
    ;; add a duplicate subscriber
    (testing "queues don't add duplicate subscribers"
      (add-subscriber queue subscriber)
      (is (= 1 (active-subscribers queue))))
    (testing "queues can have multiple subscribers"
      (add-subscriber queue subscriber2)
      (is (= 2 (active-subscribers queue))))
    (testing "queues can remove subscribers"
      (remove-subscriber queue (:sub-id subscriber))
      (is (= 1 (active-subscribers queue))))))

(deftest queue-can-handle-acking-subscribers
  (let [message1 (create-message 1 "test message 1")
	message2 (create-message 2 "test message 2")
	queue (create-queue "queue1")
	latch (java.util.concurrent.CountDownLatch. 2)
	sub-fn (fn [msg] (println msg) (.countDown latch))
	subscription1 (create-subscription 1 1 :client sub-fn)
	subscription2 (create-subscription 2 1 :client sub-fn)]
    ;; add two subscribers
    (testing "queues can properly add subscribers"
      (add-subscriber queue subscription1)
      (add-subscriber queue subscription2)
      (is (= 2 (active-subscribers queue))))
    (testing "queues send messages to available subscribers"
      ;; send one message, expect exactly one subscriber to pick it up
      (enqueue queue message1)
      (is (= 2 (active-subscribers queue)))
      (is (= 1 (pending-subscribers queue)))
      (is (= 0 (message-count queue)))
      (is (= 1 (.getCount latch)))
      ;; send another message, expect the only other subscriber to get it
      (enqueue queue message2)
      (is (= 2 (active-subscribers queue)))
      (is (= 2 (pending-subscribers queue)))
      (is (= 0 (message-count queue)))
      (is (= 0 (.getCount latch))))
    (testing "queues store messages when no active subscribers exist"
      (remove-subscriber queue (:sub-id subscription1))
      (remove-subscriber queue (:sub-id subscription2))
      (is (= 0 (active-subscribers queue)))
      (enqueue queue message2)
      (is (= 1 (message-count queue))))
    (testing "queues can remove subscribers with pending acks"
      (remove-subscriber queue (:sub-id subscription1))
      (is (= 1 (active-subscribers queue)))
      (is (= 2 (pending-subscribers queue))))))

;(deftest queue-can-handle-message-acks
;  (let [message1 (create-message 123 "test message 1")
;	queue (create-queue "queue1")
;	latch (java.util.concurrent.CountDownLatch. 2)
;	sub-fn (fn [msg] (println msg) (.countDown latch))
;	subscription1 (create-subscription 111 111 :client sub-fn)]
;    ;; add two subscribers
;    (add-subscriber queue subscription1)
;    (testing "acking a valid message-id properly works"
;      ;; enqueue one message
;      (enqueue queue message1)
;      ;; ack the first message
;      (ack-message queue (:sub-id subscription1) (:message-id message1))
;      (is (= 0 (pending-subscribers queue)))
;      (is (= 0 (message-count queue))))
;    (testing "acking an invalid message requeues and removes subscription from pending queue"
;      (enqueue queue message1)
;      (remove-subscriber queue (:sub-id subscription1))
;      (ack-message queue (:sub-id subscription1) 9999)
;      (is (= 0 (pending-subscribers queue)))
;      (is (= 1 (message-count queue))))))
