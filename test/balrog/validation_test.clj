(ns balrog.validation-test
  (:use balrog.validation
	balrog.core
	balrog.stomp
	balrog.test-utils
	clojure.test :reload-all))

(defn create-test-broker [] [(init-balrog) (create-test-connection)])

(deftest balrog-handles-improper-connect-frames
  (testing "balrog throws an error with bad protocl versions"
    (let [broker (init-balrog)
	  conn (create-test-connection)
	  bad-connect (create-bad-connect-frame)
	  [new-conn response] (validate broker conn bad-connect)]
      (is (= (:command response) :error))
      (is (= (-> response :headers :version) "1.0,1.1"))
      (is (= (:connected? new-conn) :not-connected)))))

(deftest balrog-handles-improper-send-frames
  (testing "balrog throws an error when no destination is present"
    (let [[broker conn] (create-test-broker)
	  bad-send (create-bad-send-frame)
	  [new-conn response] (validate broker conn bad-send)]
      (is (= (:command response) :error))
      (is (= (:connected? new-conn) :not-connected)))))

(deftest balrog-handles-improper-subscribe-frames
  (testing "balrog throws an error with no id is present"
    (let [[broker conn] (create-test-broker)
	  bad-subscribe (create-incomplete-subscribe-frame)
	  [new-conn response] (validate broker conn bad-subscribe)]
      (is (= (:command response) :error))))
  (testing "balrog throws an error with no destination is present"
    (let [[broker conn] (create-test-broker)
	  bad-subscribe (create-no-destination-subscribe-frame)
	  [new-conn response] (validate broker conn bad-subscribe)]
      (is (= (:command response) :error)))))

(deftest balrog-handles-improper-unsubscribe-frames
  (testing "balrog throws error when unsubscribing with an invalid id"
    (let [[broker conn] (create-test-broker)
	  frame (create-unsubscribe-frame-missing-id)
	  [new-conn response] (validate broker conn frame)]
      (is (= (:command response) :error)))))

(deftest balrog-handles-improper-ack-frames
  (testing "balrog throws error when ack frame's are missing id"
    (let [[broker conn] (create-test-broker)
	  ack-frame (create-acknowledge-frame-no-id "/queue/test" 1234)
	  [new-conn response] (validate broker conn ack-frame)]
      (is (= (:command response) :error))))
  (testing "balrog throws error when acknowledge frame's missing message-id"
    (let [[broker conn] (create-test-broker)
	  ack-frame (create-acknowledge-frame-no-message-id "/queue/test" 123)
	  [new-conn response] (validate broker conn ack-frame)]
      (is (= (:command response) :error)))))

(deftest balrog-handles-improper-nack-frames
  (testing "balrog throws error when nack frame's are missing id"
    (let [[broker conn] (create-test-broker)
	  nack-frame (create-nack-frame-no-id "/queue/test" 1234)
	  [new-conn response] (validate broker conn nack-frame)]
      (is (= (:command response) :error))))
  (testing "balrog throws error when nack frame's missing message-id"
    (let [[broker conn] (create-test-broker)
	  nack-frame (create-nack-frame-no-message-id "/queue/test" 123)
	  [new-conn response] (validate broker conn nack-frame)]
      (is (= (:command response) :error)))))
      

