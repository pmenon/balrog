(ns balrog.subscription
  (:use [balrog.ring-buffer :as ring]))

(defrecord Subscription [sub-id client-sub-id ack sub-fn])

(defn create-subscription [sub-id client-sub-id ack sub-fn]
  (Subscription. sub-id client-sub-id ack sub-fn))

(defprotocol SubscriptionListProto
  (curr-sub [this])
  ;(next-active-sub [this])
  (add-sub [this sub])
  (remove-sub [this sub])
  (deactivate-sub [this sub])
  (activate-sub [this sub])
  (count-pending [this])
  (count-active [this]))

(defrecord SubscriptionList [current active pending]
  ring/Rotatable
  (lrot [this]
    (let [nex (peek active)
	  res (lrot active)]
      (SubscriptionList. nex res pending)))

  SubscriptionListProto
  (curr-sub [this]
    (:current this))
;  (next-active-sub [this]
;    (when (< (count (:pending this)) (count (:active this)))
;      (loop [active-subs (:active this)];
;	(if-let [s (find
;	  c
;	  (recur (lrot s))))))
  (add-sub [this sub]
    (SubscriptionList. (if (= (:empty (:current this))) sub (:current sub))
		       (conj (:active this) sub)
		       (:pending this)))
  (remove-sub [this sub])
  (deactivate-sub [this sub]
    (SubscriptionList. (:current this) (:active this) (assoc (:pending this) sub sub)))
  (activate-sub [this sub]
    (SubscriptionList. (:current this) (:active this) (dissoc (:pending this) sub)))
  (count-pending [this]
    (count (:pending this)))
  (count-active [this]
    (count (:active this))))

(defn make-subscription-list []
  (SubscriptionList. :empty clojure.lang.PersistentQueue/EMPTY {}))

