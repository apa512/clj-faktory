(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [clj-faktory.core :refer :all]))

(deftest core-test
  (let [worker-manager (worker-manager {:concurrency 1
                                        :heartbeat 400})]
    (start worker-manager)
    (prn (perform-async worker-manager {:jid "12345678"
                                        :jobtype :hej
                                        :args [:github 319058]}))
    (stop worker-manager)))
