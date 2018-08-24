(ns event-data-landing-page-sampler.http
  "Web fetching."
  (:require [org.httpkit.client :as http]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [config.core :refer [env]])
  (:import [java.net URI]
           [javax.net.ssl SNIHostName SNIServerName SSLEngine SSLParameters]))

; (def redirect-depth 4)
; 
(def user-agent "CrossrefEventDataBot (https://www.crossref.org/services/event-data/;mailto:labs@crossref.org)")

(def headers {"Referer" "https://eventdata.crossref.org"
              "User-Agent" user-agent})

(defn sni-configure
  [^SSLEngine ssl-engine ^URI uri]
  (let [^SSLParameters ssl-params (.getSSLParameters ssl-engine)]
    (.setServerNames ssl-params [(SNIHostName. (.getHost uri))])
    (.setSSLParameters ssl-engine ssl-params)))

(def sni-client (http/make-client
                  {:ssl-configurer sni-configure}))
