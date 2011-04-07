(ns balrog.util
  (:import (java.util UUID)))

(defn assoc-or [map key val]
  (if (map key)
    map
    (assoc map key val)))

(defn update-with [map key fn]
  (if (map key)
    map
    (let [old (map key)
	  new (fn old)]
      (if (= old new) map (assoc map key new)))))

(defn generateUUID []
  (.toString (UUID/randomUUID)))