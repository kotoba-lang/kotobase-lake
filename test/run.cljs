(ns run
  "nbb entry point for the portable suite -- the same shape
  `kotobase-storage` uses, and the cheap one to run locally.

  `test/kotobase/lake/cljs_runner.cljs` is the other half: `cljs.main` under
  real ClojureScript compilation, which is what CI gates on. Both exist
  because they fail differently -- nbb is SCI-interpreted and will not catch
  an advanced-compilation or a self-host-only defect, and cljs.main takes
  minutes on a loaded machine."
  (:require [clojure.test :as t]
            [kotobase.lake.catalog-test]
            [kotobase.lake.compact-test]
            [kotobase.lake.reader-test]
            [kotobase.lake.sniff-test]
            [kotobase.lake.acquire-test]
            [kotobase.lake.table-test]
            [kotobase.lake.tabular-test]))

(defmethod t/report [::t/default :end-run-tests] [m]
  (when-not (t/successful? m)
    (js/process.exit 1)))

(t/run-tests 'kotobase.lake.sniff-test
             'kotobase.lake.catalog-test 'kotobase.lake.compact-test
             'kotobase.lake.reader-test
             'kotobase.lake.tabular-test
             'kotobase.lake.acquire-test
             'kotobase.lake.table-test)
