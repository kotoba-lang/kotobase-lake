(ns kotobase.lake.table
  "A table is N files. This is what makes the lake a lake.

  Everything below here reads one object well. A lake's table is a set of them
  under a prefix — `region=east/part-0.parquet`, a thousand siblings — and a
  query over it must not open a thousand footers to answer a question about
  three files.

  ## The manifest is datoms, like everything else

  Per member: its object, its row count, its **partition values**, and its
  **per-column statistics**. All on the same plane as the catalog, so
  \"which files did this tenant land last week, and what is the price range in
  them\" is one query rather than a directory walk plus a thousand reads.

      member/table            table:<tenant>|<name>
      member/object           obj:<cid>
      member/rows             1000
      member/partition/region \"east\"
      member/min/price        10
      member/max/price        30
      member/nulls/price      0

  Attribute-per-column, because that is what makes it joinable. A blob of EDN
  under one attribute would be a manifest the query plane cannot see into.

  ## Pruning a file is the same operation as pruning a row group

  `columnar.stats/skip?` decides whether a chunk can be ruled out from its
  min/max. A file's statistics have the same shape, so file-level pruning is
  **the same function one level up** — including the rule that matters most:
  absent statistics never permit a skip. A manifest entry with no bounds for a
  column means that file gets opened, which is slower and correct, and the
  alternative is rows silently missing from answers.

  ## Partition columns are not in the files

  `region=east/part-0.parquet` has no `region` column; the value is in the
  path. So two things follow, and both are load-bearing:

  1. A predicate on a partition column prunes **without opening anything** —
     no footer, no range read, nothing. This is the cheapest pruning in the
     system and the reason partitioning exists.
  2. A query that *asks for* the partition column has to be answered from the
     manifest, because no file contains it. Row counts are in the manifest and
     subjects are derived from the object id, so those datoms are synthesised
     without a single byte of any file.

  ## Members are opened lazily, per scan

  `open-member` is called for survivors only, inside `-scan`. Building the
  table source opens nothing. Constructing sources eagerly and merging them
  would be the natural shape and would defeat the entire namespace — the
  pruning would be perfect and every file would already be open."
  (:require [clojure.string :as str]
            [columnar.stats :as cstats]
            [datom.source :as source]
            [kotobase.lake.catalog :as catalog]))

(def ^:private sep "|")

(defn table-id [tenant name] (str "table:" tenant sep name))
(defn member-id [table cid] (str "member:" table sep cid))

(def ^:private partition-prefix "member/partition/")
(def ^:private min-prefix "member/min/")
(def ^:private max-prefix "member/max/")
(def ^:private nulls-prefix "member/nulls/")

(defn add-member
  "Quads describing one file's membership in a table.

  `:statistics` is `{column {:rows n :nulls n :min v :max v}}` — whatever the
  writer recorded. A column absent from the map, or present without bounds, is
  a column this member cannot be pruned on. That is a normal state and the
  reason `columnar.stats` treats missing bounds as \"no claim\"."
  [{:keys [table cid rows partition statistics]}]
  (let [mid (member-id table cid)]
    (cond-> #{{:s mid :p "member/table" :o table}
              {:s mid :p "member/object" :o (catalog/object-id cid)}
              {:s mid :p "member/rows" :o (long rows)}}
      true (into (map (fn [[col v]] {:s mid :p (str partition-prefix col) :o v}))
                 partition)
      true (into (mapcat (fn [[col {:keys [nulls min max]}]]
                           (cond-> [{:s mid :p (str nulls-prefix col) :o (long (or nulls 0))}]
                             (some? min) (conj {:s mid :p (str min-prefix col) :o min})
                             (some? max) (conj {:s mid :p (str max-prefix col) :o max}))))
                 statistics))))

(defn- strip [p prefix] (subs p (count prefix)))

(defn members
  "Every member of `table`, as maps the pruner and the source both use.

  Sorted by member id. The manifest is a SET, so without this the order is
  whatever the collection iterates in — and that order reaches query plans,
  the sequence files are opened in, and the order rows come back when a caller
  imposes none. Arbitrary-but-stable is not what a set gives; it gives
  arbitrary-and-liable-to-change, which is the kind of thing that makes a
  reproduction stop reproducing."
  [quads table]
  (->> quads
       (filter #(and (= "member/table" (:p %)) (= table (:o %))))
       (map :s)
       distinct
       sort
       (mapv (fn [mid]
               (let [e (catalog/entity quads mid)]
                 {:member mid
                  :object (get e "member/object")
                  :cid (subs (get e "member/object") (count "obj:"))
                  :rows (long (get e "member/rows" 0))
                  :partition (into {} (keep (fn [[p v]]
                                              (when (str/starts-with? p partition-prefix)
                                                [(strip p partition-prefix) v])))
                                   e)
                  :statistics
                  (reduce (fn [acc [p v]]
                            (cond
                              (str/starts-with? p min-prefix)
                              (assoc-in acc [(strip p min-prefix) :min] v)
                              (str/starts-with? p max-prefix)
                              (assoc-in acc [(strip p max-prefix) :max] v)
                              (str/starts-with? p nulls-prefix)
                              (assoc-in acc [(strip p nulls-prefix) :nulls] v)
                              :else acc))
                          {} e)})))))

(defn- stats-of
  "A member's statistics for `column`, in the shape `columnar.stats` reads, or
  nil when the manifest makes no claim about it."
  [{:keys [rows statistics]} column]
  (when-let [s (get statistics column)]
    (when (or (contains? s :min) (contains? s :max) (contains? s :nulls))
      (merge {:rows rows :nulls 0} s))))

(defn skip-member?
  "Can this member be ruled out for `predicates` without opening it?

  Partition first, because it is free: the value is in the manifest and no
  file has to exist for the comparison. Then column statistics, through the
  same `columnar.stats/skip?` that prunes row groups — a file and a row group
  differ in scale and not in kind."
  [member predicates]
  (boolean
   (or (some (fn [[op col v]]
               (when-let [pv (get (:partition member) col)]
                 (case op
                   := (not= v pv)
                   ;; A partition value this namespace cannot compare is not
                   ;; grounds to skip. Ranges over partition values are a
                   ;; real optimisation and deliberately not guessed at here.
                   false)))
             predicates)
       (cstats/skip? #(stats-of member %) predicates))))

(defn prune
  "-> `{:live [..] :skipped n}`."
  [members predicates]
  (let [live (vec (remove #(skip-member? % predicates) members))]
    {:live live :skipped (- (count members) (count live))}))

(defn- pattern->predicates [[_ p o]]
  (if (and (some? p) (some? o)) [[:= p o]] []))

(defn- partition-datoms
  "Partition values as datoms, synthesised from the manifest.

  No file contains its partition column, so this is the only place these can
  come from — and row counts plus the object id are enough, which means a
  query for a partition column reads nothing at all."
  [{:keys [object rows partition]} [s p o]]
  (into #{}
        (for [[col v] partition
              :when (and (or (nil? p) (= p col)) (or (nil? o) (= o v)))
              i (range rows)
              :let [subject (str object "#row" i)]
              :when (or (nil? s) (= s subject))]
          {:s subject :p col :o v})))

(defn table-source
  "An `IPatternSource` over every file in a table.

  `open-member` is `(fn [member] -> IPatternSource)` and is called **only for
  members that survive pruning**, memoised so repeated scans do not reopen a
  file. `on-prune` is an optional `(fn [{:keys [live skipped pattern]}])` —
  tests use it to prove that pruning happened, which the answers alone cannot
  show."
  [{:keys [quads table open-member on-prune]}]
  (let [ms (members quads table)
        opened (atom {})
        open! (fn [m] (or (get @opened (:member m))
                          (let [s (open-member m)]
                            (swap! opened assoc (:member m) s)
                            s)))]
    (reify source/IPatternSource
      (-scan [_ pattern]
        (let [preds (pattern->predicates pattern)
              {:keys [live skipped]} (prune ms preds)
              partition-cols (into #{} (mapcat (comp keys :partition)) ms)
              [_ p _] pattern]
          (when on-prune (on-prune {:live (mapv :cid live) :skipped skipped
                                    :pattern pattern}))
          (if (and (some? p) (contains? partition-cols p))
            ;; Answerable from the manifest alone. Opening a file to look for
            ;; a column that is not in it would be the whole cost of the query
            ;; for none of the answer.
            (into #{} (mapcat #(partition-datoms % pattern)) live)
            (into #{}
                  (mapcat (fn [m]
                            (into (source/scan (open! m) pattern)
                                  ;; An unbound predicate still wants the
                                  ;; partition columns back with the rest.
                                  (when (nil? p) (partition-datoms m pattern)))))
                  live)))))))
