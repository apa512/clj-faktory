# clj-faktory

Faktory workers in Clojure!

## Install

[![Clojars Project](https://clojars.org/tech.ducktype/clj-faktory/latest-version.svg)](http://clojars.org/com.apa512/rethinkdb)

## Usage

```clojure
(require '[clj-faktory.core :as faktory])

(faktory/defjob :say-hello [first-name]
  (println "Hello" first-name "!"))
  
(let [worker (faktory/worker "tcp://localhost:7419" {:concurrency 5})]
  (faktory/perform-async worker :say-hello ["Erik"])
  (faktory/start worker))
```
