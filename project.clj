(defproject event-data-landing-page-sampler "0.1.0-SNAPSHOT"
  :description "Event Data Landing Page Sampler"
  :url "http://eventdata.crossref.org/"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [crossref-util "0.1.15"]
                 [clj-http "3.4.1"]
                 [clj-http-fake "1.0.3"]
                 [http-kit "2.3.0-alpha5"]
                 [robert/bruce "0.8.0"]
                 [yogthos/config "0.8"]
                 [clj-time "0.12.2"]
                 [event-data-common "0.1.57"]
                 [org.apache.httpcomponents/httpclient "4.5.2"]
                 [org.apache.commons/commons-io "1.3.2"]
                 [org.clojure/tools.logging "0.3.1"]
                 [slingshot "0.12.2"]
                 [cc.qbits/spandex "0.6.0"]
                 [com.climate/claypoole "1.1.4"]
                 [org.clojure/core.async "0.4.474"]
                 [enlive "1.1.6"]]
  :main ^:skip-aot event-data-landing-page-sampler.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
