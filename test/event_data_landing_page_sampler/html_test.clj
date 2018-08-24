(ns event-data-landing-page-sampler.html-test
  (:require [clojure.test :refer :all]
            [event-data-landing-page-sampler.html :as html]))

(deftest ambiguous-doi
  (testing "Empty list returns nil."
    (is (= (html/unambiguous-doi [])
           nil)))

  (testing "Non-doi list returns empty."
    (is (= (html/unambiguous-doi [["dc.identifier" "pmid1234"]])
           nil)))

  (testing "Same DOI repeatedly not ambiguous."
    (is (= (html/unambiguous-doi [["dc.identifier" "10.5555/12345678"]
                                 ["dc.identifier" "10.5555/12345678"]])
            "10.5555/12345678")))

  (testing "Same DOI repeatedly, different meta tags not ambiguous."
    (is (= (html/unambiguous-doi [["citation_doi" "10.5555/12345678"]
                                 ["dc.identifier" "10.5555/12345678"]])
            "10.5555/12345678")))

  (testing "DOI with other identifiers not ambiguous."
    (is (= (html/unambiguous-doi [["dc.identifier" "10.5555/12345678"]
                                 ["dc.identifier" "pmid1234"]
                                 ["dc.identifier" "10.5555/12345678"]])
            "10.5555/12345678")))

  (testing "DOI with other identifiers and different DOI expressions not ambiguous."
    (is (= (html/unambiguous-doi [["dc.identifier" "10.5555/12345678"]
                                 ["dc.identifier" "pmid1234"]
                                 ["dc.identifier" "https://dx.doi.org/10.5555/12345678"]])
            "10.5555/12345678")))

  (testing "Different DOI causes a conflict."
    (is (= (html/unambiguous-doi [["citation_doi" "10.5555/12345678"]
                                 ["dc.identifier" "10.6666/12345678"]])
            "ambiguous")))


  (testing "Very strange content is bad. e.g. http://ologyjournals.com/aaeoa/aaeoa_00008.php"
    (is (= (html/unambiguous-doi [["citation_doi" "/"] ["dc.identifier" "doi:/"]])
            "bad"))))

(deftest doi-in-html-text
  (testing "Best version recognised."
    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello https://doi.org/10.5555/12345678 my https://dx.doi.org/10.5555/12345678 name http://dx.doi.org/10.5555/12345678 is http://doi.org/10.5555/12345678 Josiah 10.5555/12345678 Carberry.")
        "doi-https"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello my https://dx.doi.org/10.5555/12345678 name http://dx.doi.org/10.5555/12345678 is http://doi.org/10.5555/12345678 Josiah 10.5555/12345678 Carberry.")
        "doi-http"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello my https://dx.doi.org/10.5555/12345678 name http://dx.doi.org/10.5555/12345678 is Josiah 10.5555/12345678 Carberry.")
        "dx-https"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello my name http://dx.doi.org/10.5555/12345678 is Josiah 10.5555/12345678 Carberry.")
        "dx-http"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello my name is Josiah 10.5555/12345678 Carberry.")
        "non-url"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello my name is Josiah Carberry.")
        "missing"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "Hello my name is <a href='http://doi.org/10.5555/12345678'>Josiah Carberry</a>.")
        "missing")
        "Content not part of the body text isn't taken into account."))

  (testing "Input DOI to match is normalized."
    (is (= (html/doi-in-html-text "https://doi.org/10.5555/12345678" "https://doi.org/10.5555/12345678")
        "doi-https"))

    (is (= (html/doi-in-html-text "10.5555/12345678" "https://doi.org/10.5555/12345678")
        "doi-https"))

    (is (= (html/doi-in-html-text "http://dx.doi.org/10.5555/12345678" "https://doi.org/10.5555/12345678")
        "doi-https"))))






(deftest doi-hyperlinked
  (testing "Best version recognised."
    (is (= (html/doi-hyperlinked "10.5555/12345678" "Hello <a href='https://doi.org/10.5555/12345678'>ahem</a> my <a href='https://dx.doi.org/10.5555/12345678'>ahem</a> name <a href='http://dx.doi.org/10.5555/12345678'>ahem</a> is <a href='http://doi.org/10.5555/12345678'>ahem</a. Josiah Carberry.")
        "doi-https"))

    (is (= (html/doi-hyperlinked "10.5555/12345678" "Hello my <a href='https://dx.doi.org/10.5555/12345678'>ahem</a> name <a href='http://dx.doi.org/10.5555/12345678'>ahem</a> is <a href='http://doi.org/10.5555/12345678'>ahem</a> Josiah Carberry.")
        "doi-http"))

    (is (= (html/doi-hyperlinked "10.5555/12345678" "Hello my <a href='https://dx.doi.org/10.5555/12345678'>ahem</a> name <a href='http://dx.doi.org/10.5555/12345678'>ahem</a> is Josiah Carberry.")
        "dx-https"))

    (is (= (html/doi-hyperlinked "10.5555/12345678" "Hello my name <a href='http://dx.doi.org/10.5555/12345678'>ahem</a> is Josiah Carberry.")
        "dx-http"))

    (is (= (html/doi-hyperlinked "10.5555/12345678" "Hello my name is Josiah Carberry.")
        "missing")))

  (testing "Input DOI to match is normalized."
    (is (= (html/doi-hyperlinked "https://doi.org/10.5555/12345678" "<a href='https://doi.org/10.5555/12345678'>ahem</a>")
        "doi-https"))

    (is (= (html/doi-hyperlinked "10.5555/12345678" "<a href='https://doi.org/10.5555/12345678'>ahem</a>")
        "doi-https"))

    (is (= (html/doi-hyperlinked "http://dx.doi.org/10.5555/12345678" "<a href='https://doi.org/10.5555/12345678'>ahem</a>")
        "doi-https")))


  (testing "SICIs handled, URL encoded"
    (is (= (html/doi-hyperlinked
             "10.1676/0043-5643(2001)113[0327:FSOTEC]2.0.CO;2"
             "<p class='articleRef'>The Wilson Bulletin 113(3):327-328. 2001<br/><a href='https://doi.org/10.1676/0043-5643(2001)113[0327:FSOTEC]2.0.CO;2'>https://doi.org/10.1676/0043-5643(2001)113[0327:FSOTEC]2.0.CO;2</a></p>")
        "doi-https"))

    (is (= (html/doi-hyperlinked
             "10.1676/0043-5643(2001)113[0327:FSOTEC]2.0.CO;2"
             "<p class='articleRef'>The Wilson Bulletin 113(3):327-328. 2001<br/><a href='https://doi.org/10.1676/0043-5643%282001%29113%5B0327%3AFSOTEC%5D2.0.CO%3B2'>https://doi.org/10.1676/0043-5643(2001)113[0327:FSOTEC]2.0.CO;2</a></p>")
        "doi-https"))))

