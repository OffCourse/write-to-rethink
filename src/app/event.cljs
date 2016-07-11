(ns app.event
  (:require [clojure.string :as str]))

(defn convert-payload [data]
  (map #(-> js/JSON
            (.parse (.toString (js/Buffer. %1 "base64") "ascii"))
            (js->clj :keywordize-keys true)) data))

(defn extract-payload [event]
  (map #(-> %1 :kinesis :data) (:Records event)))

(defn event->payload [event]
  (-> event
      (extract-payload)
      (convert-payload)))

(defn convert [event]
  (let [event (js->clj event :keywordize-keys true)
        payload (event->payload event)
        event-source (-> event
                         :Records
                         first
                         :eventSourceARN
                         (str/split "/")
                         last)]
    {:records payload
     :event-source event-source}))
