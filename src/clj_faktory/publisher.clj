(ns clj-faktory.publisher
  (:require [clj-faktory.client :as client]
            [clj-faktory.utils :as utils]
            [crypto.random :as random]))

(defn publish
  ([publisher job-type args opts]
   (let [jid (random/hex 12)
         job (cond-> (merge {:jid jid
                             :jobtype job-type
                             :args args
                             :queue "default"
                             :retry 25
                             :backtrace 10}
                            opts)
               (utils/transit-args? args) (utils/encode-transit-args))]
     (client/push (::conn publisher) job)
     jid))
  ([publisher job-type args]
   (publish publisher job-type args {})))

(defn publisher [uri]
  (let [info (utils/client-info)
        conn (.connect (client/connection uri info))]
    {::info info
     ::opts {:uri uri}
     ::conn conn}))