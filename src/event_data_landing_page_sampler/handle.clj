(ns event-data-landing-page-sampler.handle
  "Handle API for DOIs"
  (:require [org.httpkit.client :as client]
            [event-data-landing-page-sampler.http :as http]
            [config.core :refer [env]]
            [robert.bruce :refer [try-try-again]]
            [crossref.util.doi :as cr-doi]
            [event-data-common.storage.store :as store]
            [event-data-common.storage.redis :as redis]
            [clojure.tools.logging :as log]
            [event-data-common.evidence-log :as evidence-log]
            [clojure.data.json :as json])
  (:import [java.net URL URI URLEncoder]
           [javax.net.ssl SNIHostName SNIServerName SSLEngine SSLParameters]))




(defn resolve-doi
  "Resolve and validate a DOI or ShortDOI, expressed as not-URL form. May or may not be URLEscaped. Return the DOI."
  [doi]

  (let [doi (cr-doi/non-url-doi doi)
        response (try-try-again
                    {:sleep 5000 :tries 2}
                    #(deref (client/get
                              (str "https://doi.org/api/handles/" (URLEncoder/encode doi "UTF-8"))
                              {:as :text
                               :client http/sni-client
                               :headers http/headers})))

        status (:status response)

        body (when (= 200 status)
               (-> response :body (json/read-str :key-fn keyword)))
        
        url (when body
              (->> body :values (filter #(= (:type %) "URL")) first :data :value))]
      (log/info "Resolved DOI" doi "to" url)
      (when-not url
        (log/error "Couldn't get URL for DOI" doi))
    url))


