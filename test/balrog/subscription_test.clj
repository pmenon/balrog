(ns balrog.subscription-test
  (:use [balrog.subscription]
	[clojure.test] :reload))

(deftest can-get-current-sub-from-subscription
  (let [s (make-subscription-list)
	s1 (create-subscription nil nil nil nil)]
    (= (get-next-sub (add-sub s s1)) s1)))