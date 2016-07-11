(ns app.db
  (:require [cljs.nodejs :as node]
            [cljs.core.match :refer-macros [match]]
            [cljs.core.async :refer [<! put! close! chan >!]]
            [clojure.string :as str])
  (:require-macros [cljs.core.async.macros :refer [go]]))

(def r (node/require "rethinkdb"))

(def config {:host (.. js/process -env -RETHINK_HOST)
                :port (.. js/process -env -RETHINK_PORT)
                :ssl {:ca (.. js/process -env -RETHINK_CERT)}
                :authKey (.. js/process -env -RETHINK_AUTH_KEY)})

(defn connect []
  (let [c (chan)]
    (.connect r (clj->js config)
              #(go
                 (if %1
                   (>! c {:error %1})
                   (>! c {:connection %2}))))
    c))

(defn insert [conn table-name data]
  (let [c (chan)
        db    (.db r "offcourse")
        table (.table db table-name)
        opp  (.insert table (clj->js data))]
    (.run opp conn #(go (>! c (or %1 %2))))
    c))
