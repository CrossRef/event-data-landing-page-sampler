(ns event-data-landing-page-sampler.core-test
  (:require [clojure.test :refer :all]
            [event-data-landing-page-sampler.core :as core]))



(deftest analysis-best
  (let [snapshot {:pid/doi "10.5555/12345678"}]
    
    ; Perfect example.
    (is (= (core/analyze snapshot
             "<html>
                <head>
                  <meta name='dc.identifier' content='doi:10.5555/12345678' />
                </head>
                <body>
                  My DOI is: <a href='https://doi.org/10.5555/12345678'>https://doi.org/10.5555/12345678</a> what's yours?
                </body>
              </html>")
        {:pid/doi "10.5555/12345678"
         :analysis/meta-tags [["dc.identifier" "doi:10.5555/12345678"]]
         :analysis/meta-tag-correctness "correct"
         :analysis/doi-in-text "doi-https"
         :analysis/doi-hyperlinked "doi-https"}))

    ; Linked in body, but not to display guidelines.
    (is (= (core/analyze snapshot
             "<html>
                <head>
                  <meta name='dc.identifier' content='doi:10.5555/12345678' />
                </head>
                <body>
                  My DOI is: <a href='http://dx.doi.org/10.5555/12345678'>https://doi.org/10.5555/12345678</a> what's yours?
                </body>
              </html>")
        {:pid/doi "10.5555/12345678"
         :analysis/meta-tags [["dc.identifier" "doi:10.5555/12345678"]]
         :analysis/meta-tag-correctness "correct"
         :analysis/doi-in-text "doi-https"
         :analysis/doi-hyperlinked "dx-http"}))

    ; Unlinked but present in in body text.
    (is (= (core/analyze snapshot
             "<html>
                <head>
                  <meta name='dc.identifier' content='doi:10.5555/12345678' />
                </head>
                <body>
                  My DOI is: https://doi.org/10.5555/12345678 what's yours?
                </body>
              </html>")
        {:pid/doi "10.5555/12345678"
         :analysis/meta-tags [["dc.identifier" "doi:10.5555/12345678"]]
         :analysis/meta-tag-correctness "correct"
         :analysis/doi-in-text "doi-https"
         :analysis/doi-hyperlinked "missing"}))

    ; Completely absent from body.
    (is (= (core/analyze snapshot
             "<html>
                <head>
                  <meta name='dc.identifier' content='doi:10.5555/12345678' />
                </head>
                <body>
                  My DOI is secret.
                </body>
              </html>")
        {:pid/doi "10.5555/12345678"
         :analysis/meta-tags [["dc.identifier" "doi:10.5555/12345678"]]
         :analysis/meta-tag-correctness "correct"
         :analysis/doi-in-text "missing"
         :analysis/doi-hyperlinked "missing"}))


    ; In body but not meta tag.
    (is (= (core/analyze snapshot
             "<html>
                <body>
                  My DOI is: <a href='https://doi.org/10.5555/12345678'>https://doi.org/10.5555/12345678</a> what's yours?
                </body>
              </html>")
        {:pid/doi "10.5555/12345678"
         :analysis/meta-tags []
         :analysis/meta-tag-correctness "missing"
         :analysis/doi-in-text "doi-https"
         :analysis/doi-hyperlinked "doi-https"}))
    
    

        )
  )