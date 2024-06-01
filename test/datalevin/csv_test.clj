(ns datalevin.csv-test
  (:require
   [clojure.java.io :as io]
   [datalevin.csv :as sut]
   [clojure.test :refer [deftest is are]])
  (:import
   [java.io Reader StringReader StringWriter EOFException]))

(deftest buffer-size-test
  (let [input  "1,movie\n2,tv series\n3,tv movie\n4,video movie\n5,tv mini series\n6,video game\n7,episode"
        output [["1","movie"]
                ["2","tv series"]
                ["3","tv movie"]
                ["4","video movie"]
                ["5","tv mini series"]
                ["6","video game"]
                ["7","episode"]]]
    (are [size] (= output (sut/read-csv input {:cb-size size}))
      1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24)))

(deftest line-ending-test
  (let [lf-end    "1,actor\n2,actress\n3,producer\n4,writer\n5,cinematographer\n6,composer\n7,costume designer\n8,director\n9,editor\n10,miscellaneous crew\n11,production designer\n12,guest\n"
        cr-end    "1,actor\r2,actress\r3,producer\r4,writer\r5,cinematographer\r6,composer\r7,costume designer\r8,director\r9,editor\r10,miscellaneous crew\r11,production designer\r12,guest\r"
        crlf-end  "1,actor\r\n2,actress\r\n3,producer\r\n4,writer\r\n5,cinematographer\r\n6,composer\r\n7,costume designer\r\n8,director\r\n9,editor\r\n10,miscellaneous crew\r\n11,production designer\r\n12,guest\r\n"
        mixed-end "1,actor\n2,actress\r\n3,producer\r\n4,writer\r5,cinematographer\r\n6,composer\r\n7,costume designer\r\n8,director\r\n9,editor\r\n10,miscellaneous crew\r\n11,production designer\r\n12,guest"
        output    [["1","actor"]
                   ["2","actress"]
                   ["3","producer"]
                   ["4","writer"]
                   ["5","cinematographer"]
                   ["6","composer"]
                   ["7","costume designer"]
                   ["8","director"]
                   ["9","editor"]
                   ["10","miscellaneous crew"]
                   ["11","production designer"]
                   ["12","guest"]]]
    (are [input] (= output (sut/read-csv input))
      lf-end cr-end crlf-end mixed-end)))

(deftest quotation-test
  (let [output [["28866" "Toivo Moisio, \"Topi\"" "" "" "T1523" "T1"]
                ["88773" "\"Plane Doctor\"" "" "" "P4532" "D236"]
                ["96149" "Gabriel \"Gabo\" Williams" "" "" "G1642" "W452"]
                ["234" "Happy .\\/. Halow" "" "" "G1642" "S52"]
                ["831" "Bob \"danger\" Smith" "" "" "O869" "T928"]]]
    (is (= output
           (with-open [reader (io/reader "test/data/test.csv")]
             (sut/read-csv reader))))))

;; minor modification from tests of clojure.data.csv

(def ^{:private true} simple
  "Year,Make,Model
1997,Ford,E350
2000,Mercury,Cougar
")

(def ^{:private true} simple-alt-sep
  "Year;Make;Model
1997;Ford;E350
2000;Mercury;Cougar
")

(def ^{:private true} complicated
  "1997,Ford,E350,\"ac, abs, moon\",3000.00
1999,Chevy,\"Venture \"\"Extended Edition\"\"\",\"\",4900.00
1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",\"\",5000.00
1996,Jeep,Grand Cherokee,\"MUST SELL!
air, moon roof, loaded\",4799.00")

(deftest reading-test
  (let [csv (sut/read-csv simple)]
    (is (= (count csv) 3))
    (is (= (count (first csv)) 3))
    (is (= (first csv) ["Year" "Make" "Model"]))
    (is (= (last csv) ["2000" "Mercury" "Cougar"])))
  (let [csv (sut/read-csv simple-alt-sep :separator \;)]
    (is (= (count csv) 3))
    (is (= (count (first csv)) 3))
    (is (= (first csv) ["Year" "Make" "Model"]))
    (is (= (last csv) ["2000" "Mercury" "Cougar"])))
  (let [csv (sut/read-csv complicated)]
    (is (= (count csv) 4))
    (is (= (count (first csv)) 5))
    (is (= (first csv)
           ["1997" "Ford" "E350" "ac, abs, moon" "3000.00"]))
    (is (= (last csv)
           ["1996" "Jeep" "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded" "4799.00"]))))


(deftest reading-and-writing-test
  (let [string-writer (StringWriter.)]
    (->> simple sut/read-csv (sut/write-csv string-writer))
    (is (= simple (str string-writer)))))

(deftest parse-line-endings-test
  (let [csv (sut/read-csv "Year,Make,Model\n1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv))))
  (let [csv (sut/read-csv "Year,Make,Model\r\n1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv))))
  (let [csv (sut/read-csv "Year,Make,Model\r1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv))))
  (let [csv (sut/read-csv "Year,Make,\"Model\"\r1997,Ford,E350")]
    (is (= 2 (count csv)))
    (is (= ["Year" "Make" "Model"] (first csv)))
    (is (= ["1997" "Ford" "E350"] (second csv)))))
