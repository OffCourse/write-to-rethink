(ns app.core
  (:require [cljs.nodejs :as node]
            [cljs.core.match :refer-macros [match]]
            [cljs.core.async :refer [<! put! close! chan >!]]
            [clojure.string :as str])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(node/enable-util-print!)
(def r (node/require "rethinkdb"))
(def db-config {:host (.. js/process -env -RETHINK_HOST)
                :port (.. js/process -env -RETHINK_PORT)
                :ssl {:ca (.. js/process -env -RETHINK_CERT)}
                :authKey (.. js/process -env -RETHINK_AUTH_KEY)})

(defn convert-payload [data]
  (println data)
  (map #(-> js/JSON
            (.parse (.toString (js/Buffer. %1 "base64") "ascii"))
            (js->clj :keywordize-keys true)) data))

(defn extract-payload [event]
  (map #(-> %1 :kinesis :data) (:Records event)))

(defn event->payload [event]
  (-> event
      (extract-payload)
      (convert-payload)))

(defn convert-event [event]
  (let [event (js->clj event :keywordize-keys true)
        payload (event->payload event)
        event-source (-> event
                         :Records
                         first
                         :eventSourceARN
                         (str/split "/")
                         last)]
    {:payload payload
     :event-source event-source}))

(defn db-connect []
  (let [c (chan)]
    (.connect r (clj->js db-config)
              #(go
                 (if %1
                   (>! c {:error %1})
                   (>! c {:connection %2}))))
    c))

(defn handle-response [res]
  (match [res]
         [{:connection _}] (:connection res)
         [{:error _}] (do
                        (println (:error res))
                        nil)))

(defn insert-bookmarks [conn table-name data]
  (let [c (chan)
        db    (.db r "offcourse")
        table (.table db table-name)
        opp  (.insert table (clj->js data))]
    (.run opp conn #(go (>! c (or %1 %2))))
    c))

(defn extract-records [payload]
  payload)

(defn ^:export handler [event context cb]
  (println (.stringify js/JSON event))
  (go
    (let [{:keys [payload event-source]} (convert-event event)
          connection (-> (<! (db-connect))
                         handle-response )
          records (extract-records payload)
          response (<! (insert-bookmarks connection "bookmarks" records))]
      (.close connection #(cb nil (clj->js response))))))

(defn -main [] identity)
(set! *main-cli-fn* -main)
