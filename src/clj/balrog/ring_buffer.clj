(ns balrog.ring-buffer)

(defprotocol Rotatable
  (lrot [this]))

(extend-protocol Rotatable
  clojure.lang.PersistentQueue
  (lrot [this]
    (let [p (peek this)]
      (conj (pop this) p))))

;  balrog.subscription.SubscriptionList
;  (lrot [this]
;    (let [nex (peek (:active this))
;	  res (lrot (:active this))
;	  pen (:pending this)]
;       (SubscriptionList. nex res pen))))