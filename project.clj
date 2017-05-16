(defproject orca "0.1.0-SNAPSHOT"
  :description "Encode Clojure data structures into Apache ORC"
  :url "http://github.com/bpoweski/orca"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.apache.orc/orc-core "1.3.3"]]
  :profiles {:dev {:resource-paths ["test-resources"]}})
