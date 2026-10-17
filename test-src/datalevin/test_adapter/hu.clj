(ns ^:no-doc datalevin.test-adapter.hu
  "Selected-backend ordered-key codecs; training remains explicit fixture setup."
  (:require [datalevin.hu]
            [datalevin.test-adapter :as adapter])
  (:import [datalevin.hu HuTucker]))

(deftype Dictionary [session id])

(defn codes->hu-tucker [lengths codes]
  (adapter/call! :hu/codes->hu-tucker [lengths codes]))

(defn from-native
  "Use a native training/hand-built fixture's codes with the selected codec."
  [^HuTucker fixture]
  (codes->hu-tucker (.-lens fixture) (.-codes fixture)))

(defn encode [dictionary source destination]
  (adapter/call! :hu/encode [dictionary source destination]))

(defn decode [dictionary source destination]
  (adapter/call! :hu/decode [dictionary source destination]))

(defn dump-hu-tucker [dictionary path]
  (adapter/call! :hu/dump-hu-tucker [dictionary path]))

(defn load-hu-tucker [input]
  (adapter/call! :hu/load-hu-tucker [input]))
