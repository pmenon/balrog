(ns balrog.store)

(defprotocol Store
  (store-message [msg])
  (get-message [msg-id])
  (get-messages [msg-ids]))

