(ns app.core
  (:require [cljs.nodejs :as node]
            [cljs.core.match :refer-macros [match]]
            [cljs.core.async :refer [<! put! close! chan >!]])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(node/enable-util-print!)
(def r (node/require "rethinkdb"))
(def db-config {:host (.. js/process -env -RETHINK_HOST)
                :port (.. js/process -env -RETHINK_PORT)
                :ssl {:ca (.. js/process -env -RETHINK_CERT)}
                :authKey (.. js/process -env -RETHINK_AUTH_KEY)})

(defn convert-payload [data]
  (-> js/JSON
      (.parse (.toString (js/Buffer. data "base64") "ascii"))
      (js->clj :keywordize-keys true)))

(defn extract-payload [event]
  (-> (:Records event)
      first
      first
      second
      :data))

(defn event->payload [event]
  (-> event
      (js->clj :keywordize-keys true)
      (extract-payload)
      (convert-payload)
      (dissoc :id)))

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

(defn insert-bookmark [conn bookmark]
  (let [c (chan)
        db    (.db r "offcourse")
        table (.table db "bookmarks")
        opp  (.insert table (clj->js bookmark))]
    (.run opp conn #(go (>! c (or %1 %2))))
    c))


(defn ^:export handler [event context cb]
  (go
    (let [payload (event->payload event)
          connection (-> (<! (db-connect))
                         handle-response )
          response (<! (insert-bookmark connection payload))]
      (.close connection #(cb nil (clj->js response))))))

(defn -main [] identity)
(set! *main-cli-fn* -main)
