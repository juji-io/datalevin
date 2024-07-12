(ns datalevin-bench.core-test
  (:require [datalevin-bench.core :as sut]
            [datalevin.core :as d]
            [clojure.test :as t :refer [are deftest]]))

(deftest correct-results-test
  (let [conn (d/get-conn "db")
        db   (d/db conn)]
    (are [query result] (= (d/q query db) result)
      sut/q-1a [["(A Warner Bros.-First National Picture) (presents)"
                 "A Clockwork Orange" 1934]]
      sut/q-1b [["(Set Decoration Rentals) (uncredited)" "Disaster Movie" 2008]]
      sut/q-1c [["(co-production)" "Intouchables" 2011]]
      sut/q-1d [["(Set Decoration Rentals) (uncredited)" "Disaster Movie" 2004]]
      sut/q-2a [["'Doc'"]]
      sut/q-2b [["'Doc'"]]
      sut/q-2c []
      sut/q-2d [["& Teller"]]
      sut/q-3a [["2 Days in New York"]]
      sut/q-3b [["300: Rise of an Empire"]]
      sut/q-3c [["& Teller 2"]]
      sut/q-4a [["5.1" "& Teller 2"]]
      sut/q-4b [["9.1" "Batman: Arkham City"]]
      sut/q-4c [["2.1" "& Teller 2"]]
      sut/q-5a []
      sut/q-5b []
      sut/q-5c [["11,830,420"]]
      sut/q-6a [["Downey Jr., Robert" "Iron Man 3"]]
      sut/q-6b [["based-on-comic" "Downey Jr., Robert" "The Avengers 2"]]
      sut/q-6c [["Downey Jr., Robert" "The Avengers 2"]]
      sut/q-6d [["based-on-comic" "Downey Jr., Robert" "2008 MTV Movie Awards"]]
      sut/q-6e [["Downey Jr., Robert" "Iron Man 3"]]
      sut/q-6f [["based-on-comic" "\"Steff\", Stefanie Oxmann Mcgaha" "& Teller 2"]]
      sut/q-7a [["Antonioni, Michelangelo" "Dressed to Kill"]]
      sut/q-7b [["De Palma, Brian" "Dressed to Kill"]]
      sut/q-7c [["50 Cent" "\"Boo\" Arnold was born Earl Arnold in Hattiesburg, Mississippi in 1966. His father gave him the nickname 'Boo' early in life and it stuck through grade school, high school, and college. He is still known as \"Boo\" to family and friends.  Raised in central Texas, Arnold played baseball at Texas Tech University where he graduated with a BA in Advertising and Marketing. While at Texas Tech he was also a member of the Texas Epsilon chapter of Phi Delta Theta fraternity. After college he worked with Young Life, an outreach to high school students, in San Antonio, Texas.  While with Young Life Arnold began taking extension courses through Fuller Theological Seminary and ultimately went full-time to Gordon-Conwell Theological Seminary in Boston, Massachusetts. At Gordon-Conwell he completed a Master's Degree in Divinity studying Theology, Philosophy, Church History, Biblical Languages (Hebrew & Greek), and Exegetical Methods. Following seminary he was involved with reconciliation efforts in the former Yugoslavia shortly after the war ended there in1995.  Arnold started acting in his early thirties in Texas. After an encouraging visit to Los Angeles where he spent time with childhood friend George Eads (of CSI Las Vegas) he decided to move to Los Angeles in 2001 to pursue acting full-time. While in Los Angeles he has studied acting with Judith Weston at Judith Weston Studio for Actors and Directors.  Arnold's acting career has been one of steady development, booking co-star and guest-star roles in nighttime television. He guest-starred opposite of Jane Seymour on the night time television drama Justice. He played the lead, Michael Hollister, in the film The Seer, written and directed by Patrick Masset (Friday Night Lights).  He was nominated Best Actor in the168 Film Festival for the role of Phil Stevens in the short-film Useless. In Useless he played a US Marshal who must choose between mercy and justice as he confronts the man who murdered his father. Arnold's performance in Useless confirmed his ability to carry lead roles, and he continues to work toward solidifying himself as a male lead in film and television.  Arnold married fellow Texan Stacy Rudd of San Antonio in 2003 and they are now raising their three children in the Los Angeles area."]]
      )))
