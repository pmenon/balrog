(ns balrog.ring-buffer)

(defprotocol Rotatable
  (lrot [this]))

(extend-protocol Rotatable
  clojure.lang.PersistentQueue
  (lrot [this]
    (if (> (.size this) 1)
      (conj (pop this) (peek this))
      this)))

;  balrog.subscription.SubscriptionList
;  (lrot [this]
;    (let [nex (peek (:active this))
;	  res (lrot (:active this))
;	  pen (:pending this)]
;       (SubscriptionList. nex res pen))))