(ns balrog.performance-test
  (:use balrog.server
        balrog.core
        balrog.stomp
        balrog.test-utils
        clojure.test :reload-all))

(defn test-balrog [nproducers nconsumers nmsgs]
  (let [niters (/ nmsgs nproducers)
	broker (init-balrog)
	latch (java.util.concurrent.CountDownLatch. nmsgs)
	conns (vec
	       (map (fn [_] (atom (create-test-connection (fn [_] (.countDown latch))))) (range nconsumers)))
	send-frame (create-send-frame "/queue/test")
	subscribe-frame (partial create-subscribe-frame "/queue/test")]
    ;; start consumers
    (dotimes [i nconsumers] (on-subscribe broker (nth conns i) (subscribe-frame i)))
    (dorun
     (apply pcalls
	    (repeat nproducers
		    #(dotimes [_ niters] (handle-frame broker (nth conns 0) send-frame)))))
    (.await latch)))

(deftest test-balrog-throughput
  (time
   (test-balrog 10 10 1000000)))

;(deftest test-balrog-throughput
;  (let [nthreads 10
;	niters 100000
;        broker (init-balrog)
;        latch (java.util.concurrent.CountDownLatch. (* niters nthreads))
;        conn (atom (create-test-connection (fn [_] (.countDown latch))))
;        send-frame (create-send-frame "/queue/test")
;	subscribe-frame (partial create-subscribe-frame "/queue/test")]
;    (dotimes [i 10]
;      (on-subscribe broker conn (subscribe-frame i)))
;    (time (do
;     (dorun
;       (apply pcalls
;	      (repeat nthreads
;		      #(dotimes [_ niters] (handle-frame broker conn send-frame))))
;       (.await latch))))))
     
