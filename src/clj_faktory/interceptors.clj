(ns clj-faktory.interceptors
  (:require [clj-faktory.protocol.transit :as transit]))

(defn- encode-args [job]
  (-> job
      (update :args (comp vector transit/write))
      (assoc-in [:custom :args-encoding] "transit")))

(defn- decode-args [job]
  (if (= (get-in job [:custom :args-encoding]) "transit")
    (update job :args (comp transit/read first))
    job))

(def transit-args
  {:enter encode-args
   :leave decode-args})
