(ns kotobase.lake.cljs-runner
  "ClojureScript entry point for the portable suite.

  `cljs.main --target node` rather than nbb/shadow-cljs, matching
  `datom-source`: this library has one zero-dep runtime dependency and needs
  neither npm nor a build tool. `run-tests` is wrapped so a failing suite
  exits non-zero -- a runner that swallows the exit code turns a red gate
  green forever."
  (:require [clojure.test :as t]
            [kotobase.lake.catalog-test]
            [kotobase.lake.compact-test]
            [kotobase.lake.docs-test]
            [kotobase.lake.rdf-test]
            [kotobase.lake.reader-test]
            [kotobase.lake.sniff-test]
            [kotobase.lake.acquire-test]
            [kotobase.lake.table-test]
            [kotobase.lake.tabular-test]))

(defmethod t/report [::t/default :end-run-tests] [m]
  (when-not (t/successful? m)
    (js/process.exit 1)))

(defn -main [& _]
  (t/run-tests 'kotobase.lake.sniff-test
               'kotobase.lake.catalog-test 'kotobase.lake.compact-test
               'kotobase.lake.docs-test
               'kotobase.lake.rdf-test 'kotobase.lake.reader-test
             'kotobase.lake.tabular-test
             'kotobase.lake.acquire-test
             'kotobase.lake.table-test))
