(ns balrog.validation
  (:use [balrog.stomp]))

(defmulti validate (fn [broker conn frame] (:command frame)))

(defn is-supported1? [versions client-version]
  (let [client-versions (.split client-version ",")]
    (reduce #(or %1 (contains? versions (Double/parseDouble %2))) false client-versions)))

(defmethod validate :connect [broker conn frame]
  (let [client-version (-> frame :headers :accept-version)
	supported-versions (:supported-versions broker)]
    (if (and client-version (not (is-supported1? supported-versions client-version)))
      [conn (error-frame {:version (reduce #(str %1 "," %2) supported-versions)}
			 "Get your version game up, son!")]
      [conn frame])))

(defmethod validate :send [broker conn frame]
  (let [destination (-> frame :headers :destination)]
    (if-not destination
      [conn (error-frame {} "Error: Destination is required for send frames.")]
      [conn frame])))

(defmethod validate :subscribe [broker conn frame]
  (let [destination (-> frame :headers :destination)
	id (-> frame :headers :id)
	existing-sub? (contains? (:subs conn) id)]
    [conn
     (cond
      (not destination) (error-frame {} "Error: Destination is required for subscribe frames.")
      (not id) (error-frame {} "Error: ID is required for subscriber frames.")
      existing-sub? (error-frame {} "Error: A subscription with that ID already exists.")
      :else frame)]))

(defmethod validate :unsubscribe [broker conn frame]
  (let [client-sub-id (-> frame :headers :id)
	[sub destination] (get-in conn [:subs client-sub-id])
	sub-id (:sub-id sub)]
    [conn
     (cond
      (not sub-id) (error-frame {} "Error, the subscription does not exist")
      (not destination) (error-frame {} "Error: Destination is required for subscribe frames.")
      (not client-sub-id) (error-frame {} "Error: ID is required for subscriber frames.")
      :else frame)]))

(defmethod validate :ack [broker conn frame]
  (let [message-id (-> frame :headers :message-id)
	client-sub-id (-> frame :headers :id)
	client-txn-id (-> frame :headers :transaction)]
    [conn
     (cond
      (not message-id) (error-frame {} "Error: Message ID is required for ACK frames.")
      (not client-sub-id) (error-frame {} "Error: ID is required for ACK frames.")
      (and client-txn-id (get-in conn [:txns client-txn-id])) frame
      :else (error-frame {} "Error: The provided transaction does not exist."))]))

(defmethod validate :nack [broker conn frame]
  (let [message-id (-> frame :headers :message-id)
	client-sub-id (-> frame :headers :id)
	client-txn-id (-> frame :headers :transaction)]
    [conn
     (cond
      (not message-id) (error-frame {} "Error: Message ID is required for NACK frames.")
      (not client-sub-id) (error-frame {} "Error: ID is required for NACK frames.")
      (and client-txn-id (get-in conn [:txns client-txn-id])) frame
      :else (error-frame {} "Error: The provided transaction does not exist."))]))

(defmethod validate :begin [broker conn frame]
  (let [txn-id (-> frame :headers :transaction)]
    [conn
     (cond
      (not txn-id) (error-frame {} "Error: Transaction ID is required for BEGIN frames.")
      :else frame)]))

(defmethod validate :commit [broker conn frame]
  (let [txn-id (-> frame :headers :transaction)]
    [conn
     (cond
      (not txn-id) (error-frame {} "Error: Transaction ID is required for COMMIT frames.")
      :else frame)]))

(defmethod validate :abort [broker conn frame]
  (let [txn-id (-> frame :headers :transaction)]
    [conn
     (cond
      (not txn-id) (error-frame {} "Error: Transaction ID is required for ABORT frames."))]))

(defmethod validate :disconnect [broker conn frame]
  [conn frame])
