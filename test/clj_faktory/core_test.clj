(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [clj-faktory.core :refer :all]))

(deftest core-test
  (let [client (connect)
        worker-manager (worker-manager client {:concurrency 1
                                               :heartbeat 15000})]
    (prn (perform-async worker-manager {:jid "12345678"
                                        :jobtype :profile/github
                                        :args [:github 319058]}))
    (Thread/sleep 1000)
    (stop worker-manager)))
