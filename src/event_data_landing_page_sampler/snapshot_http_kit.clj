(ns event-data-landing-page-sampler.snapshot-http-kit
  (:require [org.httpkit.client :as client]
            [event-data-landing-page-sampler.http :as http]
            [robert.bruce :refer [try-try-again]]
            [clojure.tools.logging :as log]
            [event-data-landing-page-sampler.handle :as handle]
            [config.core :refer [env]])
  (:import [org.apache.commons.codec.digest DigestUtils]))

(def redirect-depth 4)


(def timeout-ms
  "Timeout for HTTP requests."
  30000)

(def deref-timeout-ms
  "Last-ditch timeout for derefing result. This is a safety-valve to avoid threads hanging."
  60000)

(def max-body-size
  "2MB"
  (* 1024 1024 2))

(defn fetch-throwing
  "Fetch the content at a URL as a string, following redirects.
   Don't accept cookies.
   Return seq of {:url :status :body} in order of redirect."
  ([url] (fetch-throwing url url []))
  ([url original-url history]
  (if-not url
    (log/error "Failed to follow URL" original-url "got nil.")
    (try
      (if (> (count history) redirect-depth)
        history
        (let [result (deref
                       (client/get
                         url
                         {:follow-redirects false
                          :headers http/headers
                          :as :text
                          :client http/sni-client
                          :timeout timeout-ms
                          :filter (client/max-body-filter max-body-size)
                          :throw-exceptions true})
                       deref-timeout-ms
                       nil)

              new-history (conj history {:url url
                                         :status (:status result)
                                         :body (:body result)})

              location-header (-> result :headers :location)]
          
          ; Timeout results in nil, in which case return nil.
          (when result

            (condp = (:status result)
              200 new-history
              ; Weirdly some Nature pages return 401 with the content. http://www.nature.com/nrendo/journal/v10/n9/full/nrendo.2014.114.html
              401 new-history
              301 (fetch-throwing location-header original-url new-history)
              303 (fetch-throwing location-header original-url new-history)
              302 (fetch-throwing location-header original-url new-history)
              nil))))

      (catch Exception exception
        (do
          (log/error "Exception" exception)
          (.printStackTrace exception)
          nil))))))

(defn fetch
  [url]
  (try
    (try-try-again
      {:sleep 5000 :tries 2}
      #(fetch-throwing url))
    
    ; If we can't retrieve anything, mark it as empty content.
    (catch Exception _ "")))

(defn try-get-domain
  [url-str]
  (try
    (.getHost (java.net.URI. url-str))
    (catch Exception _ nil)))


(defn snapshot
  "From a pid object, create and return a snapshot object, as tuple of [snapshot-object, content]."
  [pid]
  (let [doi (:pid/doi pid)
        resource-url (handle/resolve-doi doi)
        timestamp (System/currentTimeMillis)

        history (fetch resource-url)
        content (-> history last :body)

        urls (map #(select-keys % [:url :status]) history)
        final-url (-> history last :url)
        final-url-domain (try-get-domain final-url)

        ; Salt the hash with the DOI to avoid collisions e.g. for empty content.
        ; Also handles the null case for content.
        content-hash (DigestUtils/sha1Hex ^String (str doi content))]

 [{:snapshot/resource-url resource-url
   :snapshot/last-analysis nil
   :snapshot/timestamp timestamp
   :snapshot/content-hash content-hash
   :snapshot/urls urls
   :snapshot/final-url-domain final-url-domain
   :snapshot/final-url final-url
   :snapshot/driver "http-kit"}
   content]))

