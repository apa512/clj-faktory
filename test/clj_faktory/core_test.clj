(ns clj-faktory.core-test
  (:require [clojure.test :refer :all]
            [clj-faktory.core :refer :all]))

(deftest core-test
  (let [worker-manager (worker-manager {:concurrency 1
                                        :heartbeat 400})]
    (start worker-manager)
    (prn (perform-async worker-manager {:jid "12345678"
                                        :jobtype :hej
                                        :args [1 2]}))
    (prn (fetch worker-manager))
    (stop worker-manager)
    #_(start worker-manager)
    #_(Thread/sleep 1000)
    #_(stop worker-manager)
    #_(Thread/sleep 1000)
    #_(prn "APA")
    #_(prn (core/push client :scrape-profile ["github" 319058]))
    #_(prn (fetch client ["default"]))
    #_(prn (str ::apa))
    #_(net/read-and-parse-response client)))
