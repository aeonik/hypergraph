(ns aeonik.multi-hypergraph
  "A homoiconic multi-hypergraph library for Clojure.

  CORE CONCEPTS:
  - Nodes: Entities with id, type, and optional span
  - Edges: Hyperedges connecting multiple nodes via 'pins' with roles
  - Spans: Text/document ranges for spatial indexing
  - Graphs: Immutable structures with efficient structural sharing

  PHILOSOPHY:
  - Everything is a graph (even graphs themselves)
  - Minimal core, infinite extensibility through composition
  - Overlapping spans? Another graph layer
  - Custom indices? Another graph
  - Build operations? Also a graph"
  (:require [clojure.set :as set]))

;;; ============================================================================
;;; Core Data Structure
;;; ============================================================================

(defn empty-graph
  "Create an empty multi-hypergraph.

  Structure:
  - :nodes - map of id -> node
  - :edges - map of id -> edge
  - :meta - {:next-nid N :next-eid N} for id allocation
  - :ix - indices for efficient queries:
    - :incidence - map of node-id -> #{edge-ids}
    - :by-type - {:node {type #{ids}} :edge {type #{ids}}}
    - :spans - sorted-map of start -> {:end E :ids #{} :kind :node/:edge}"
  []
  {:nodes {}
   :edges {}
   :meta  {:next-nid 0 :next-eid 0}
   :ix    {:incidence {}
           :by-type   {}
           :spans     (sorted-map)}})

;;; ============================================================================
;;; Internal Helpers
;;; ============================================================================

(defn- alloc
  "Allocate next ID from counter. Returns [id updated-graph]."
  [g k]
  (let [id (get-in g [:meta k])]
    [id (assoc-in g [:meta k] (inc id))]))

(defn- index-span
  "Add span to index. Spans at same position merge into single slot."
  [g kind id {:keys [start end]}]
  (if (and start end)
    (update-in g [:ix :spans start]
               (fnil (fn [slot]
                       (-> slot
                           (update :ids (fnil conj #{}) id)
                           (assoc :end end :kind kind)))
                     {:end end :ids #{id} :kind kind}))
    g))

(defn- deindex-span
  "Remove span from index."
  [g kind id {:keys [start]}]
  (if-let [slot (get-in g [:ix :spans start])]
    (let [ids (disj (:ids slot) id)]
      (if (seq ids)
        (assoc-in g [:ix :spans start] (assoc slot :ids ids))
        (update-in g [:ix :spans] dissoc start)))
    g))

;;; ============================================================================
;;; Core Operations (Immutable API)
;;; ============================================================================

(defn add-node
  "Add a node to the graph.

  Node map keys:
  - :id (optional) - specify ID, otherwise auto-allocated
  - :type (required) - node type for indexing
  - :span (optional) - {:start N :end N} for spatial indexing
  - ... any other keys stored as-is

  Example:
    (add-node g {:type :element
                 :name \"div\"
                 :span {:start 0 :end 100}})"
  [g {:keys [id type span] :as node}]
  (let [[id g] (if (some? id) [id g] (alloc g :next-nid))
        node   (assoc node :id id)]
    (-> g
        (assoc-in  [:nodes id] node)
        (update-in [:ix :by-type :node type] (fnil conj #{}) id)
        (index-span :node id span)
        (update-in [:meta :next-nid] (fnil max 0) (inc id)))))

(defn add-edge
  "Add a hyperedge connecting multiple nodes.

  Edge map keys:
  - :id (optional) - specify ID, otherwise auto-allocated
  - :type (required) - edge type for indexing
  - :pins (required) - vector of pin maps, each with:
    - :node - node ID to connect
    - :role - semantic role in the edge
    - :i (optional) - index for ordering
  - :attrs (optional) - metadata map
  - :span (optional) - {:start N :end N} for spatial edges

  Example:
    (add-edge g {:type :child
                 :pins [{:node 0 :role :parent}
                        {:node 1 :role :child :i 0}
                        {:node 2 :role :child :i 1}]})"
  [g {:keys [id type pins attrs span] :as edge}]
  (doseq [p pins]
    (when-not (get-in g [:nodes (:node p)])
      (throw (ex-info "Pin references missing node" {:edge id :pin p}))))
  (let [[id g] (if (some? id) [id g] (alloc g :next-eid))
        edge'  (-> edge
                   (assoc :id id :attrs (or attrs {}) :pins (vec pins)))]
    (as-> g g
      (assoc-in g [:edges id] edge')
      (update-in g [:ix :by-type :edge type] (fnil conj #{}) id)
      (reduce (fn [g {:keys [node]}]
                (update-in g [:ix :incidence node] (fnil conj #{}) id))
              g pins)
      (index-span g :edge id span)
      (update-in g [:meta :next-eid] (fnil max 0) (inc id)))))

(defn rem-edge
  "Remove an edge and update all indices."
  [g eid]
  (let [edge (get-in g [:edges eid])]
    (when-not edge (throw (ex-info "No such edge" {:eid eid})))
    (as-> g g
      (update-in g [:ix :by-type :edge (:type edge)] (fnil disj #{}) eid)
      (reduce (fn [g {:keys [node]}]
                (update-in g [:ix :incidence node] disj eid))
              g (:pins edge))
      (deindex-span g :edge eid (:span edge))
      (update g :edges dissoc eid))))

(defn rem-node
  "Remove a node and all incident edges."
  [g nid]
  (when-not (get-in g [:nodes nid])
    (throw (ex-info "No such node" {:nid nid})))
  (let [edges (get-in g [:ix :incidence nid] #{})
        g'    (reduce rem-edge g edges)
        node  (get-in g' [:nodes nid])]
    (as-> g' g
      (update-in g [:ix :by-type :node (:type node)] (fnil disj #{}) nid)
      (deindex-span g :node nid (:span node))
      (update g :nodes dissoc nid)
      (update-in g [:ix :incidence] dissoc nid))))

;;; ============================================================================
;;; Queries
;;; ============================================================================

(defn incident-edges
  "Get all edge IDs connected to a node."
  [g nid]
  (get-in g [:ix :incidence nid] #{}))

(defn neighbors
  "Find neighbor nodes connected via edges.

  Options:
  - edge-type: filter by edge type
  - role: filter by pin role

  Examples:
    (neighbors g 0)                    ; all neighbors
    (neighbors g 0 :child nil)         ; via :child edges
    (neighbors g 0 :child :parent)     ; parents in :child edges"
  ([g nid] (neighbors g nid nil nil))
  ([g nid edge-type role]
   (let [eids (incident-edges g nid)]
     (->> eids
          (keep (fn [eid]
                  (let [e (get-in g [:edges eid])]
                    (when (or (nil? edge-type) (= (:type e) edge-type)) e))))
          (mapcat (fn [e]
                    (for [p (:pins e)
                          :when (and (not= (:node p) nid)
                                     (or (nil? role) (= role (:role p))))]
                      (:node p))))
          distinct vec))))

(defn edges-of-type [g t] (get-in g [:ix :by-type :edge t] #{}))
(defn nodes-of-type [g t] (get-in g [:ix :by-type :node t] #{}))

(defn find-id
  "Find first node ID matching predicate."
  [g pred]
  (some (fn [[id n]] (when (pred n) id)) (:nodes g)))

(defn covering [g x]
  (let [m (get-in g [:ix :spans])]
    (some (fn [[start {:keys [end ids kind]}]]
            (when (< x end)
              {:kind kind :start start :end end :ids ids}))
          (rsubseq m <= x))))

(defn covering-all
  "All nodes/edges whose span covers x.
  Returns vector of {:kind :node/:edge :id id :start s :end e}."
  [g x]
  (let [m (get-in g [:ix :spans])]
    (->> (subseq m <= x)
         (reduce (fn [acc [s {:keys [end ids kind]}]]
                   (if (< x end)
                     (into acc (map (fn [id]
                                      {:kind kind :id id :start s :end end})
                                    ids))
                     acc))
                 [])
         vec)))

;;; ============================================================================
;;; Traversals
;;; ============================================================================

(defn bfs-seq
  "Breadth-first traversal of the graph from given root node(s), yielding node ids in traversal order.

  Arguments:
    g      - Graph map
    roots  - Collection of starting node ids

  Options map (optional):
    :edge-type  - Restrict traversal to edges of this type
    :directed?  - If true, traverse only along edges where a pin of role `from-role` connects the current node,
                  and pins of role `to-role` point to neighbors. For undirected traversal, all edges are traversed.
    :from-role  - Role considered as the edge direction 'from' the current node (default :from)
    :to-role    - Role considered as the edge direction 'to' neighbor nodes (default :to)

  Returns:
    A lazy sequence of node ids, in BFS order, starting from the given roots. Each node is visited at most once.

  Example:
    (bfs-seq graph [0])                            ; BFS from node 0 over all edges
    (bfs-seq graph [0] {:edge-type :child})        ; Traverse only :child edges
    (bfs-seq graph [0] {:directed? true
                        :from-role :parent
                        :to-role   :child})        ; Traverse 'child' direction"
  ([g roots] (bfs-seq g roots {}))
  ([g roots {:keys [edge-type directed? from-role to-role]
             :or   {from-role :from, to-role :to}}]
   (letfn [(nbrs [nid]
             (let [eids (get-in g [:ix :incidence nid] #{})]
               (sequence
                (comp
                 (map #(get-in g [:edges %]))
                 (filter #(or (nil? edge-type) (= (:type %) edge-type)))
                 (mapcat (fn [e]
                           (for [p (:pins e)
                                 :let [m (:node p)]
                                 :when (and (not= m nid)
                                            (if directed?
                                              (and (some #(and (= nid (:node %))
                                                               (= from-role (:role %))) (:pins e))
                                                   (= to-role (:role p)))
                                              true))]
                             m))))
                eids)))]
     (let [valid-roots (->> (distinct roots)
                            (filter #(contains? (:nodes g) %)))
           q0   (into clojure.lang.PersistentQueue/EMPTY valid-roots)
           seen (volatile! (set valid-roots))]
       (letfn [(step [q]
                 (lazy-seq
                  (when (seq q)
                    (let [nid (peek q)
                          ns  (remove @seen (nbrs nid))]
                      (vswap! seen into ns)
                      (cons nid (step (into (pop q) ns)))))))]
         (step q0))))))

(defn dfs-seq
  "Depth-first traversal of the graph from given root node(s), yielding node ids in traversal order.

  Arguments:
    g      - Graph map
    roots  - Collection of starting node ids

  Options map (optional):
    :edge-type  - Restrict traversal to edges of this type
    :directed?  - If true, traverse only along edges where a pin of role `from-role` connects the current node,
                  and pins of role `to-role` point to neighbors. For undirected traversal, all edges are traversed.
    :from-role  - Role considered as the edge direction 'from' the current node (default :from)
    :to-role    - Role considered as the edge direction 'to' neighbor nodes (default :to)

  Returns:
    A lazy sequence of node ids, in DFS order, starting from the given roots. Each node is visited at most once.

  Example:
    (dfs-seq graph [0])                            ; DFS from node 0 over all edges
    (dfs-seq graph [0] {:edge-type :child})        ; Traverse only :child edges
    (dfs-seq graph [0] {:directed? true
                        :from-role :parent
                        :to-role   :child})        ; Traverse 'child' direction"
  ([g roots] (dfs-seq g roots {}))
  ([g roots {:keys [edge-type directed? from-role to-role]
             :or   {from-role :from, to-role :to}}]
   (letfn [(nbrs [nid]
             (let [eids (get-in g [:ix :incidence nid] #{})]
               (sequence
                (comp
                 (map #(get-in g [:edges %]))
                 (filter #(or (nil? edge-type) (= (:type %) edge-type)))
                 (mapcat (fn [e]
                           (for [p (:pins e)
                                 :let [m (:node p)]
                                 :when (and (not= m nid)
                                            (if directed?
                                              (and (some #(and (= nid (:node %))
                                                               (= from-role (:role %))) (:pins e))
                                                   (= to-role (:role p)))
                                              true))]
                             m))))
                eids)))]
     (let [valid-roots (->> (distinct roots)
                            (filter #(contains? (:nodes g) %)))
           stack (vec valid-roots)
           seen  (volatile! (set valid-roots))]
       (letfn [(step [stk]
                 (lazy-seq
                  (when (seq stk)
                    (let [nid (peek stk)
                          reststk (pop stk)
                          ns (remove @seen (nbrs nid))]
                      (vswap! seen into ns)
                      (cons nid (step (into reststk ns)))))))]
         (step stack))))))

;;; ============================================================================
;;; Batch Operations with Transients
;;; ============================================================================

(defn begin
  "Begin a batch operation, returning a builder context."
  [g]
  {:nodes* (transient (:nodes g))
   :edges* (transient (:edges g))
   :inc*   (transient (get-in g [:ix :incidence] {}))
   :btn*   (transient (get-in g [:ix :by-type :node] {}))
   :bte*   (transient (get-in g [:ix :by-type :edge] {}))
   :spans-base (get-in g [:ix :spans] (sorted-map))
   :spans* (transient {})
   :next-nid (get-in g [:meta :next-nid] 0)
   :next-eid (get-in g [:meta :next-eid] 0)})

(defn- spans-upsert! [b kind id {:keys [start end]}]
  (if (and start end)
    (let [sb   (:spans-base b)
          sm   (:spans* b)
          slot (or (get sm start)
                   (get sb start)
                   {:end end :ids #{} :kind kind})
          slot' (-> slot
                    (update :ids conj id)
                    (assoc :end end :kind kind))
          sm'  (assoc! sm start slot')]
      (assoc b :spans* sm'))
    b))

(defn add-node!
  "Add node in batch mode. Returns [builder node-id]."
  [b {:keys [id type span] :as node}]
  (let [explicit? (some? id)
        id        (long (if explicit? id (:next-nid b)))
        node      (assoc node :id id)
        nodes*    (assoc! (:nodes* b) id node)
        btn*      (let [m (:btn* b) s (get m type #{})]
                    (assoc! m type (conj s id)))
        b         (-> b
                      (assoc :nodes* nodes*)
                      (assoc :btn*   btn*)
                      (update :next-nid #(max (or % 0) (inc id)))
                      (spans-upsert! :node id span))]
    [b id]))

(defn add-edge!
  "Add edge in batch mode. Returns [builder edge-id]."
  [b {:keys [id type pins attrs span] :as edge}]
  (doseq [{:keys [node]} pins]
    (when-not (contains? (:nodes* b) node)
      (throw (ex-info "Pin references missing node" {:edge id :pin node}))))
  (let [explicit? (some? id)
        id        (long (if explicit? id (:next-eid b)))
        edge'     (-> edge (assoc :id id :attrs (or attrs {}) :pins (vec pins)))
        edges*    (assoc! (:edges* b) id edge')
        bte*      (let [m (:bte* b) s (get m type #{})]
                    (assoc! m type (conj s id)))
        inc*      (reduce (fn [m {:keys [node]}]
                            (assoc! m node (conj (get m node #{}) id)))
                          (:inc* b) pins)
        b         (-> b
                      (assoc :edges* edges*)
                      (assoc :bte*   bte*)
                      (assoc :inc*   inc*)
                      (update :next-eid #(max (or % 0) (inc id)))
                      (spans-upsert! :edge id span))]
    [b id]))

(defn finish
  "Complete batch operation, returning immutable graph."
  [b]
  (let [nodes (persistent! (:nodes* b))
        edges (persistent! (:edges* b))
        inc   (persistent! (:inc* b))
        btn   (persistent! (:btn* b))
        bte   (persistent! (:bte* b))
        spans (let [delta (persistent! (:spans* b))
                    merged (reduce-kv (fn [m k v] (assoc m k v))
                                      (:spans-base b)
                                      delta)]
                (into (sorted-map) merged))]
    {:nodes nodes
     :edges edges
     :meta  {:next-nid (:next-nid b) :next-eid (:next-eid b)}
     :ix    {:incidence inc
             :by-type   {:node btn :edge bte}
             :spans     spans}}))

(defn build
  "Ergonomic batch building with a function.

  Example:
    (build g (fn [b]
               (let [[b alice] (add-node! b {:type :person :name \"Alice\"})
                     [b bob]   (add-node! b {:type :person :name \"Bob\"})
                     [b _]     (add-edge! b {:type :knows
                                             :pins [{:node alice :role :knower}
                                                    {:node bob   :role :known}]})]
                 b)))"
  [g f]
  (-> g begin f finish))

;;; ============================================================================
;;; Threading-friendly DSL
;;; ============================================================================

(defn begin*
  "Start building a graph with the DSL.
  Returns a state map with :b (builder) and :ids (alias->id map).

  Usage:
    (begin* (empty-graph))        ; Start fresh
    (begin* existing-graph)       ; Continue building on existing graph"
  [g]
  {:b (begin g) :ids {} :base-graph g})

(defn continue*
  "Continue building on an existing graph, optionally with pre-bound aliases.

  Usage:
    (continue* g)                          ; Continue with no aliases
    (continue* g {:alice 0 :bob 1})       ; Continue with ID aliases
    (continue* g (find-aliases g :person [:name]))  ; Auto-discover aliases"
  ([g]
   (continue* g {}))
  ([g aliases]
   {:b (begin g) :ids aliases :base-graph g}))

(defn find-aliases
  "Find nodes and create aliases based on type and attribute.
  Useful for continuing work on an existing graph.

  Usage:
    (find-aliases g :person [:name])       ; {:alice 0, :bob 1}
    (find-aliases g :element [:name])      ; {:div 0, :span 1}
    (find-aliases g :team [:name] \"team-\") ; {:team-eng 0, :team-design 1}"
  ([g node-type key-path]
   (find-aliases g node-type key-path ""))
  ([g node-type key-path prefix]
   (let [nodes-of-type (nodes-of-type g node-type)]
     (reduce (fn [aliases nid]
               (let [node (get-in g [:nodes nid])
                     val (get-in node key-path)
                     alias (keyword (str prefix
                                         (-> val
                                             str
                                             clojure.string/lower-case
                                             (clojure.string/replace #"[^a-z0-9]+" "-"))))]
                 (assoc aliases alias nid)))
             {}
             nodes-of-type))))

(defn with-aliases
  "Add multiple aliases to the state at once.

  Usage:
    (-> state
        (with-aliases {:alice 0 :bob 1})
        (edge :knows [[:alice :knower] [:bob :known]]))"
  [state aliases]
  (update state :ids merge aliases))

(defn finish*
  "Finish building and return the final graph."
  [{:keys [b]}]
  (finish b))

(defn- resolve-id [ids ref]
  (cond
    (keyword? ref) (or (ids ref)
                       (throw (ex-info "Alias not bound" {:alias ref})))
    (integer? ref) ref
    :else (throw (ex-info "Pin ref must be alias keyword or integer id" {:ref ref}))))

(defn node
  "Add a node bound to an alias.

  Usage:
    (node state :alice {:type :person :name \"Alice\"})"
  [state alias props]
  (let [[b id] (add-node! (:b state) props)]
    (-> state
        (assoc :b b)
        (update :ids assoc alias id))))

(defn edge
  "Add an edge with pins; pins accept aliases or ids.

  Usage:
    (edge state :knows [[:alice :knower] [:bob :known]])
    (edge state :knows [[:alice :knower] [:bob :known]] {:since 2020})"
  ([state etype pins]
   (edge state etype pins nil))
  ([state etype pins attrs]
   (let [ids   (:ids state)
         pins' (mapv (fn [[ref role & kvs]]
                       (merge {:node (resolve-id ids ref)
                               :role role}
                              (apply hash-map kvs)))
                     pins)
         [b _] (add-edge! (:b state) {:type etype :pins pins' :attrs attrs})]
     (assoc state :b b))))

(defn child
  "Helper for parent-child relationships.

  Usage:
    (child state :parent-alias :child-alias 0)"
  [state parent child- idx]
  (edge state :child [[parent :parent] [child- :child :i idx]]))

(defn knows
  "Helper for knowledge relationships.

  Usage:
    (knows state :alice :bob)"
  [state knower known]
  (edge state :knows [[knower :knower] [known :known]]))

(defn member-of
  "Helper for membership relationships.

  Usage:
    (member-of state :person :team)"
  [state member group]
  (edge state :member-of [[member :member] [group :group]]))

;;; ============================================================================
;;; Graph Composition & Utilities
;;; ============================================================================

(defn compose-layers
  "Compose multiple graph layers into a meta-graph.

  Example:
    (compose-layers {:base text-graph
                     :annotations highlight-graph
                     :comments comment-graph})"
  [layer-map]
  (let [entries (seq layer-map)
        b0      (begin (empty-graph))
        [b ids] (reduce (fn [[b ids] [k graph]]
                          (let [[b id] (add-node! b {:type :layer :name k :graph graph})]
                            [b (assoc ids k id)]))
                        [b0 {}]
                        entries)
        g       (finish b)]
    {:meta-graph g
     :layers     layer-map
     :node-ids   ids}))

(defn subgraph
  "Extract a subgraph containing only specified nodes and their edges."
  [g node-ids]
  (let [nid-set (set node-ids)
        nodes   (select-keys (:nodes g) node-ids)
        edges   (into {} (filter (fn [[eid edge]]
                                   (every? #(nid-set (:node %)) (:pins edge)))
                                 (:edges g)))]
    (build (empty-graph)
           (fn [b]
             (let [b' (reduce (fn [b node] (first (add-node! b node)))
                              b (vals nodes))]
               (reduce (fn [b edge] (first (add-edge! b edge)))
                       b' (vals edges)))))))

(defn merge-graphs
  "Merge two graphs with ID offset to avoid conflicts."
  [g1 g2]
  (let [nid-off (get-in g1 [:meta :next-nid] 0)
        eid-off (get-in g1 [:meta :next-eid] 0)
        remap-n (fn [nid] (+ nid nid-off))
        remap-e (fn [eid] (+ eid eid-off))]
    (build g1
           (fn [b]
             (let [b (reduce (fn [b [_ node]]
                               (first (add-node! b (update node :id remap-n))))
                             b (:nodes g2))
                   b (reduce (fn [b [_ edge]]
                               (let [pins' (mapv #(update % :node remap-n) (:pins edge))]
                                 (first (add-edge! b (-> edge
                                                         (update :id remap-e)
                                                         (assoc :pins pins'))))))
                             b (:edges g2))]
               b)))))

(defn assert-graph
  "Verify graph integrity. Throws if indices are inconsistent."
  [g]
  (let [nodes (:nodes g) edges (:edges g)
        {:keys [incidence by-type spans]} (:ix g)]
    ;; Check pins reference existing nodes
    (doseq [[eid e] edges
            {:keys [node]} (:pins e)]
      (when-not (contains? nodes node)
        (throw (ex-info "Dangling pin" {:eid eid :node node}))))
    ;; Check incidence index
    (doseq [[nid eids] incidence
            :when (not (contains? nodes nid))]
      (throw (ex-info "Incidence for missing node" {:nid nid})))
    ;; Check by-type index
    (doseq [[t nids] (get by-type :node)
            nid nids]
      (when-not (contains? nodes nid)
        (throw (ex-info "by-type node mismatch" {:type t :nid nid}))))
    true))

;; -----------------------------------------------------------------------------
;; Streaming & Data-Driven Utilities (ported/adapted)
;; -----------------------------------------------------------------------------

(defn neighbors-seq
  "Lazy stream of neighbor node-ids for nid, optionally filtered.
   Options:
     edge-type  - restrict to edges of this type
     role       - restrict neighbor pins by role
   Notes: This is undirected like `neighbors`; if you need direction, use BFS/DFS
   with {:directed? true ...}."
  ([g nid] (neighbors-seq g nid nil nil))
  ([g nid edge-type role]
   (let [eids (incident-edges g nid)]
     (sequence
      (comp
       (map #(get-in g [:edges %]))
       (filter #(or (nil? edge-type) (= (:type %) edge-type)))
       (mapcat (fn [e]
                 (for [p (:pins e)
                       :let [m (:node p)]
                       :when (and (not= m nid)
                                  (or (nil? role) (= role (:role p))))]
                   m))))
      eids))))

(defn build-ops
  "Build a graph from a sequence of ops (data-driven).
   ops is a seq of:
     [:node node-map] | [:edge edge-map] | [:rem-node nid] | [:rem-edge eid]
   Returns a new graph."
  [g ops]
  (build g
         (fn [b]
           (reduce
            (fn [b [op arg]]
              (case op
                :node
                (first (add-node! b arg))

                :edge
                (first (add-edge! b arg))

                :rem-node
                (if (some? arg)
              ;; finish current batch, mutate immutably, start a fresh builder
                  (begin (rem-node (finish b) arg))
                  b)

                :rem-edge
                (if (some? arg)
                  (begin (rem-edge (finish b) arg))
                  b)

            ;; unknown op -> no-op
                b))
            b
            ops))))

(defn reindex
  "Rebuild :ix entirely from :nodes and :edges. Keeps :meta as-is."
  [g]
  (let [incidence (transient {})
        btn      (transient {})
        bte      (transient {})
        spans    (transient {})]
    ;; nodes
    (doseq [[nid node] (:nodes g)]
      (let [t (:type node)]
        (assoc! btn t (conj (get btn t #{}) nid))
        (when-let [{:keys [start end]} (:span node)]
          (when (and start end)
            (let [slot (or (get spans start) {:end end :ids #{} :kind :node})]
              (assoc! spans start (-> slot
                                      (assoc :end end :kind :node)
                                      (update :ids conj nid))))))))
    ;; edges
    (doseq [[eid edge] (:edges g)]
      (let [t (:type edge)]
        (assoc! bte t (conj (get bte t #{}) eid))
        (doseq [{:keys [node]} (:pins edge)]
          (assoc! incidence node (conj (get incidence node #{}) eid)))
        (when-let [{:keys [start end]} (:span edge)]
          (when (and start end)
            (let [slot (or (get spans start) {:end end :ids #{} :kind :edge})]
              (assoc! spans start (-> slot
                                      (assoc :end end :kind :edge)
                                      (update :ids conj eid))))))))
    (-> g
        (assoc :ix {:incidence (persistent! incidence)
                    :by-type   {:node (persistent! btn)
                                :edge (persistent! bte)}
                    :spans     (into (sorted-map) (persistent! spans))}))))

;; -----------------------------------------------------------------------------
;; Homoiconic ops (graphs that describe building graphs)
;; -----------------------------------------------------------------------------

(defn op-node
  "A node that describes a graph operation (for self-describing graphs)."
  [op-type args]
  {:type :operation
   :op   op-type
   :args args})

(defn interpret-ops
  "Execute operations stored in an ops-graph.
   Currently runs op-nodes in numeric id order (simple, deterministic).
   Supported ops: :add-node, :add-edge, :rem-node, :rem-edge"
  [ops-graph]
  (let [op-nids (sort (nodes-of-type ops-graph :operation))]
    (reduce (fn [g nid]
              (let [{:keys [op args]} (get-in ops-graph [:nodes nid])]
                (case op
                  :add-node (add-node g args)
                  :add-edge (add-edge g args)
                  :rem-node (rem-node g args)
                  :rem-edge (rem-edge g args)
                  g)))
            (empty-graph)
            op-nids)))

;; -----------------------------------------------------------------------------
;; Tree/DOM helpers
;; -----------------------------------------------------------------------------

(defn attach-element
  "Recursively attach an element tree (like DOM/AST) under optional parent.
   Node map keys:
     :name :span {:start :end} :attrs {:...} :children [child ...]
   Returns updated graph."
  [g {:keys [name span attrs children]} parent-nid k-idx]
  (let [[nid g1] (alloc g :next-nid)
        g2       (add-node g1 {:id nid :type :element :name name
                               :span span :attrs attrs})
        g3       (if parent-nid
                   (add-edge g2 {:type :child
                                 :pins [{:node parent-nid :role :parent}
                                        {:node nid        :role :child :i k-idx}]})
                   g2)]
    (loop [g g3, i 0, cs children]
      (if (seq cs)
        (recur (attach-element g (first cs) nid i)
               (inc i)
               (rest cs))
        g))))

(defn children-ordered
  "Return a vector of child node-ids of nid, ordered by pin :i."
  [g nid]
  (->> (edges-of-type g :child)
       (map #(get-in g [:edges %]))
       (keep (fn [e]
               (when (some #(and (= (:role %) :parent)
                                 (= (:node %) nid))
                           (:pins e))
                 (some #(when (= (:role %) :child) %) (:pins e)))))
       (sort-by :i)
       (mapv :node)))

;; -----------------------------------------------------------------------------
;; Curried DSL wrappers (ergonomic composition)
;; -----------------------------------------------------------------------------

(defn node*
  "Curried node adder for ->, comp, etc.
   Usage:
     (-> (begin* g) (node* :a {:type :x}) ... finish*)"
  [alias props]
  (fn [state]
    (let [[b id] (add-node! (:b state) props)]
      (-> state
          (assoc :b b)
          (update :ids assoc alias id)))))

(defn edge*
  "Curried edge creator for DSL.
   Pins accept aliases or ids. Example:
     (edge* :child [[:p :parent] [:c :child :i 0]] {:k v})"
  ([etype pins] (edge* etype pins nil))
  ([etype pins attrs]
   (fn [state]
     (let [ids (:ids state)
           pins' (mapv (fn [[ref role & kvs]]
                         (merge {:node (if (keyword? ref)
                                         (or (ids ref)
                                             (throw (ex-info "Alias not bound " {:alias ref})))
                                         (if (integer? ref) ref
                                             (throw (ex-info "Pin ref must be alias keyword or integer id " {:ref ref}))))
                                 :role role}
                                (apply hash-map kvs)))
                       pins)
           [b _] (add-edge! (:b state) {:type etype :pins pins' :attrs attrs})]
       (assoc state :b b)))))

(defn child*
  "Curried sugar for parent/child edges with index."
  [parent child- idx]
  (edge* :child [[parent :parent] [child- :child :i idx]]))

;;; ============================================================================
;;; Examples & Quick Start
;;; ============================================================================

(comment
  ;; Basic usage - people and relationships
  (def people-graph
    (build (empty-graph)
           (fn [b]
             (let [[b alice] (add-node! b {:type :person :name "Alice"})
                   [b bob]   (add-node! b {:type :person :name "Bob"})
                   [b carol] (add-node! b {:type :person :name "Carol"})
                   [b _]     (add-edge! b {:type :knows
                                           :pins [{:node alice :role :knower}
                                                  {:node bob   :role :known}]})
                   [b _]     (add-edge! b {:type :knows
                                           :pins [{:node bob :role :knower}
                                                  {:node carol :role :known}]})]
               b))))

  ;; Query neighbors
  (neighbors people-graph 0 :knows :known)  ; => [1] (Bob)

  ;; DSL style - more ergonomic for complex graphs
  (def tree-graph
    (-> (begin* (empty-graph))
        (node :root {:type :element :name "html"})
        (node :head {:type :element :name "head"})
        (node :body {:type :element :name "body"})
        (node :div1 {:type :element :name "div" :class "container"})
        (node :div2 {:type :element :name "div" :class "content"})
        (child :root :head 0)
        (child :root :body 1)
        (child :body :div1 0)
        (child :body :div2 1)
        finish*))

  ;; Building a social graph with the DSL
  (def social-graph
    (-> (begin* (empty-graph))
        (node :alice {:type :person :name "Alice" :age 30})
        (node :bob   {:type :person :name "Bob" :age 25})
        (node :carol {:type :person :name "Carol" :age 28})
        (node :team1 {:type :team :name "Engineering"})
        (node :team2 {:type :team :name "Design"})
        (knows :alice :bob)
        (knows :bob :carol)
        (member-of :alice :team1)
        (member-of :bob :team1)
        (member-of :carol :team2)
        finish*))

  ;; CONTINUING/EXTENDING an existing graph
  (def initial-graph
    (-> (begin* (empty-graph))
        (node :alice {:type :person :name "Alice"})
        (node :bob {:type :person :name "Bob"})
        (knows :alice :bob)
        finish*))

  ;; Method 1: Continue with manual aliases
  (def extended-graph-v1
    (-> (continue* initial-graph {:alice 0 :bob 1})
        (node :carol {:type :person :name "Carol"})
        (knows :bob :carol)
        (knows :carol :alice)
        finish*))

  ;; Method 2: Auto-discover aliases from existing nodes
  (def extended-graph-v2
    (let [aliases (find-aliases initial-graph :person [:name])]
      (-> (continue* initial-graph aliases)
          (node :carol {:type :person :name "Carol"})
          (knows (aliases :alice) :carol)
          (knows :carol (aliases :bob))
          finish*)))

  ;; Method 3: Mix IDs and aliases
  (def extended-graph-v3
    (-> (continue* initial-graph)
        (node :carol {:type :person :name "Carol"})
        (edge :knows [[0 :knower] [:carol :known]])  ; 0 is Alice's ID
        (edge :knows [[:carol :knower] [1 :known]])  ; 1 is Bob's ID
        finish*))

  ;; Streaming pattern - adding nodes/edges incrementally
  (defn add-person-to-graph [g person-name]
    (-> (continue* g)
        (node (keyword (clojure.string/lower-case person-name))
              {:type :person :name person-name})
        finish*))

  (def streaming-graph
    (reduce add-person-to-graph
            (empty-graph)
            ["Alice" "Bob" "Carol" "David"]))

  ;; Complex streaming with relationships
  (defn add-friendship [g person1-id person2-id]
    (-> (continue* g)
        (edge :knows [[person1-id :knower] [person2-id :known]])
        finish*))

  (def graph-with-friendships
    (-> streaming-graph
        (add-friendship 0 1)  ; Alice knows Bob
        (add-friendship 1 2)  ; Bob knows Carol
        (add-friendship 2 3)  ; Carol knows David
        (add-friendship 3 0))) ; David knows Alice

  ;; Text with spans and annotations
  (def text-with-spans
    (-> (empty-graph)
        (add-node {:type :text
                   :content "The quick brown fox"
                   :span {:start 0 :end 19}})
        (add-node {:type :noun :text "fox" :span {:start 16 :end 19}})
        (add-node {:type :adj :text "quick" :span {:start 4 :end 9}})
        (add-node {:type :adj :text "brown" :span {:start 10 :end 15}})))

  ;; Find what covers position 17
  (covering text-with-spans 17)
  ;; => {:kind :node, :start 16, :end 19, :ids #{1}}

  ;; Traverse tree breadth-first
  (take 5 (bfs-seq tree-graph [0] {:edge-type :child :role :child}))
  ;; => (0 1 2 3 4) - root, then head/body, then divs

  ;; Extract subgraph
  (def sub (subgraph people-graph [0 1]))  ; Just Alice and Bob
  (edges-of-type sub :knows)  ; => #{0} - their connection preserved

  ;; Layer composition for complex documents
  (def document
    (compose-layers
     {:text (-> (empty-graph)
                (add-node {:type :paragraph :content "Hello world"
                           :span {:start 0 :end 11}}))
      :format (-> (empty-graph)
                  (add-node {:type :bold :span {:start 0 :end 5}}))
      :comments (-> (empty-graph)
                    (add-node {:type :comment :text "Nice!"
                               :span {:start 6 :end 11}}))}))

  ;; Access layer information
  (:node-ids document)  ; => {:text 0, :format 1, :comments 2}

  ;; N-ary hyperedge example - keyboard binding
  (def keybinding-graph
    (build (empty-graph)
           (fn [b]
             (let [[b keymap] (add-node! b {:type :keymap :name "default"})
                   [b action] (add-node! b {:type :action :name "save"})
                   [b device] (add-node! b {:type :device :name "keyboard"})
                   [b ctrl]   (add-node! b {:type :key :name "ctrl"})
                   [b s-key]  (add-node! b {:type :key :name "s"})
                   [b _]      (add-edge! b {:type :binding
                                            :pins [{:node keymap :role :map}
                                                   {:node action :role :action}
                                                   {:node device :role :device}
                                                   {:node ctrl   :role :modifier :i 0}
                                                   {:node s-key  :role :key :i 1}]
                                            :attrs {:priority 10}})]
               b)))))
