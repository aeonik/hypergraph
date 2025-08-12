(ns aeonik.hypergraph
  "A homoiconic hypergraph library for Clojure.

  CORE CONCEPTS:
  - Nodes: Entities with an id, type, and optional span
  - Edges: Hyperedges connecting multiple nodes via 'pins' with roles
  - Spans: Text/document ranges for spatial indexing
  - Graphs: Immutable structures with efficient structural sharing

  PHILOSOPHY:
  - Everything is a graph (even graphs themselves)
  - Minimal core, infinite extensibility through composition
  - Overlapping spans? Another graph layer
  - Custom indices? Another graph
  - Build operations? Also a graph

  QUICK START:

  ;; Basic usage
  (def g (-> (empty-graph)
             (add-node {:type :person :name \"Alice\"})
             (add-node {:type :person :name \"Bob\"})
             (add-edge {:type :knows
                        :pins [{:node 0 :role :knower}
                               {:node 1 :role :known}]})))

  ;; Batch operations with transient
  (def g2 (build g
            (fn [b]
              (-> b
                  (add-node! {:type :doc :span {:start 0 :end 100}})
                  (add-node! {:type :doc :span {:start 50 :end 150}})))))

  ;; Graph composition for overlapping spans
  (def base (-> (empty-graph)
                (add-node {:type :text :content \"Hello world\"
                          :span {:start 0 :end 11}})))

  (def annotations (-> (empty-graph)
                       (add-node {:type :highlight :color :yellow
                                 :span {:start 0 :end 5}})
                       (add-node {:type :highlight :color :blue
                                 :span {:start 6 :end 11}})))

  (def composite (compose-layers base annotations))"
  (:require [clojure.set :as set]))

;;; ============================================================================
;;; Core Data Structure
;;; ============================================================================

(defn empty-graph
  "Create an empty hypergraph.

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
               (fnil (fn [slot] (-> slot
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
        ;; ensure no future collisions when caller provided :id
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
        edge'  (-> edge (assoc :id id :attrs (or attrs {}) :pins (vec pins)))]
    (as-> g g
      (assoc-in g [:edges id] edge')
      (update-in g [:ix :by-type :edge type] (fnil conj #{}) id)
      (reduce (fn [g {:keys [node]}]
                (update-in g [:ix :incidence node] (fnil conj #{}) id))
              g pins)
      (index-span g :edge id span)
      ;; ensure no future collisions when caller provided :id
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

(defn covering
  "Find what element covers position x in the span index.
  Returns {:kind :node/:edge :start N :end N :ids #{}} or nil."
  [g x]
  (let [m   (get-in g [:ix :spans])
        a   (subseq m <= x)
        hit (last a)]
    (when hit
      (let [[start {:keys [end ids kind]}] hit]
        (when (< x end) {:kind kind :start start :end end :ids ids})))))

(defn covering-all
  "All nodes/edges whose span covers x. Returns vector of {:kind :node/:edge :id id :start s :end e}."
  [g x]
  (let [m (get-in g [:ix :spans])]
    (->> (subseq m <= x)                   ; all starts <= x
         (reduce (fn [acc [s {:keys [end ids kind]}]]
                   (if (< x end)
                     (into acc (map (fn [id] {:kind kind :id id :start s :end end}) ids))
                     acc))
                 [])
         vec)))

;;; ============================================================================
;;; Traversals
;;; ============================================================================

(defn bfs-seq
  ([g roots] (bfs-seq g roots {}))
  ([g roots {:keys [edge-type role]}]
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
                                            (or (nil? role) (= role (:role p))))]
                             m))))
                eids)))]
     (let [q0   (into clojure.lang.PersistentQueue/EMPTY (distinct roots))
           seen (volatile! (set roots))]
       (letfn [(step [q]
                 (lazy-seq
                  (when (seq q)
                    (let [nid (peek q)
                          ns  (remove @seen (nbrs nid))]
                      (vswap! seen into ns)
                      (cons nid (step (into (pop q) ns)))))))]
         (step q0))))))

(defn dfs-seq
  ([g roots] (dfs-seq g roots {}))
  ([g roots {:keys [edge-type role] :as opts}]
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
                                            (or (nil? role) (= role (:role p))))]
                             m))))
                eids)))]
     (let [stack (vec (distinct roots))
           seen  (volatile! (set roots))]
       (letfn [(step [stk]
                 (lazy-seq
                  (when (seq stk)
                    (let [nid (peek stk)
                          reststk (pop stk)]
                      (if (contains? @seen nid)
                        (step reststk)
                        (do (vswap! seen conj nid)
                            (let [ns (remove @seen (nbrs nid))]
                              (cons nid (step (into reststk ns))))))))))]
         (step stack))))))

(defn neighbors-seq
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

(defn neighbors
  ([g nid] (vec (distinct (neighbors-seq g nid nil nil))))
  ([g nid edge-type role] (vec (distinct (neighbors-seq g nid edge-type role)))))

;;; ============================================================================
;;; Batch Operations with Transients
;;; ============================================================================

(defn begin
  "Begin a batch operation, returning a builder context.
  Use with add-node!, add-edge!, then finish."
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
             (-> b
                 (add-node! {:type :person :name \"Alice\"})
                 (add-node! {:type :person :name \"Bob\"}))))"
  [g f]
  (-> g begin f finish))

(defn build-ops
  "Build from a sequence of operations (data-driven).

  Example:
  (build-ops g [[:node {:type :person :name \"Alice\"}]
                [:node {:type :person :name \"Bob\"}]
                [:edge {:type :knows :pins [...]}]])"
  [g ops]
  (build g (fn [b]
             (reduce (fn [b [op & args]]
                       (case op
                         :node (first (apply add-node! b args))
                         :edge (first (apply add-edge! b args))
                         b))
                     b ops))))

;;; ============================================================================
;;; Graph Composition & Layers
;;; ============================================================================

(defn graph-as-node
  "Wrap an entire graph as a node for meta-graph composition.
  This enables graphs of graphs!"
  [g type]
  {:type type :graph g})

(defn compose-layers
  "Compose multiple graph layers into a meta-graph.
  Each layer becomes a node, with edges describing relationships.

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

(defn reindex
  "Rebuild :ix from :nodes and :edges. Keeps :meta as-is."
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

(defn merge-graphs
  "Merge two graphs with ID offset to avoid conflicts.

  Strategies:
  - :offset - offset g2's IDs (default)
  - :prefix - prefix g2's IDs
  - :custom - provide own ID mapping function"
  ([g1 g2] (merge-graphs g1 g2 :offset))
  ([g1 g2 strategy]
   (case strategy
     :offset
     (let [nid-off (get-in g1 [:meta :next-nid] 0)
           eid-off (get-in g1 [:meta :next-eid] 0)
           remap-n (fn [nid] (+ nid nid-off))
           remap-e (fn [eid] (+ eid eid-off))
           nodes2' (into {}
                         (map (fn [[id node]]
                                (let [id' (remap-n id)]
                                  [id' (-> node (assoc :id id'))]))
                              (:nodes g2)))
           edges2' (into {}
                         (map (fn [[id edge]]
                                (let [id'   (remap-e id)
                                      pins' (mapv #(update % :node remap-n) (:pins edge))]
                                  [id' (-> edge (assoc :id id' :pins pins'))]))
                              (:edges g2)))
           g* (-> g1
                  (update :nodes merge nodes2')
                  (update :edges merge edges2')
                  (assoc-in [:meta :next-nid]
                            (max (get-in g1 [:meta :next-nid] 0)
                                 (+ nid-off (get-in g2 [:meta :next-nid] 0))))
                  (assoc-in [:meta :next-eid]
                            (max (get-in g1 [:meta :next-eid] 0)
                                 (+ eid-off (get-in g2 [:meta :next-eid] 0)))))]
       (reindex g*))
     (throw (ex-info "Unknown merge strategy" {:strategy strategy})))))
;;; ============================================================================
;;; Self-Describing Operations (Homoiconic)
;;; ============================================================================

(defn op-node
  "Create a node representing a graph operation.
  The graph can describe its own construction!"
  [op-type args]
  {:type :operation
   :op   op-type
   :args args})

(defn interpret-ops
  "Execute a graph of operations to build a new graph.

  Example:
  (def ops-graph
    (-> (empty-graph)
        (add-node (op-node :add-node {:type :person :name \"Alice\"}))
        (add-node (op-node :add-node {:type :person :name \"Bob\"}))
        (add-node (op-node :add-edge {:type :knows :pins [...]}))
        (add-edge {:type :depends-on :pins [...]}))) ; order deps

  (def result (interpret-ops ops-graph))"
  [ops-graph]
  (let [op-nodes (nodes-of-type ops-graph :operation)
        ;; TODO: topological sort based on :depends-on edges
        sorted-ops op-nodes]
    (reduce (fn [g nid]
              (let [node (get-in ops-graph [:nodes nid])
                    {:keys [op args]} node]
                (case op
                  :add-node (add-node g args)
                  :add-edge (add-edge g args)
                  :rem-node (rem-node g args)
                  :rem-edge (rem-edge g args)
                  g)))
            (empty-graph)
            sorted-ops)))

;;; ============================================================================
;;; Utilities
;;; ============================================================================

(defn attach-element
  "Helper for building tree structures (like DOM/AST).
  Recursively attaches an element and its children."
  [g {:keys [name span attrs children]} parent-nid k-idx]
  (let [[nid g1] (alloc g :next-nid)
        g2       (add-node g1 {:id nid :type :element :name name
                               :span span :attrs attrs})
        g3       (if parent-nid
                   (add-edge g2 {:type :child
                                 :pins [{:node parent-nid :role :parent}
                                        {:node nid :role :child :i k-idx}]})
                   g2)]
    (loop [g g3, i 0, cs children]
      (if (seq cs)
        (recur (attach-element g (first cs) nid i)
               (inc i)
               (rest cs))
        g))))

(defn subgraph
  "Extract a subgraph containing only specified nodes and their edges."
  [g node-ids]
  (let [nid-set (set node-ids)
        nodes   (select-keys (:nodes g) node-ids)
        edges   (into {} (filter (fn [[eid edge]]
                                   (every? #(nid-set (:node %)) (:pins edge)))
                                 (:edges g)))]
    ;; Build fresh graph with just these elements
    (build (empty-graph)
           (fn [b]
             (let [b' (reduce (fn [b node] (first (add-node! b node)))
                              b (vals nodes))]
               (reduce (fn [b edge] (first (add-edge! b edge)))
                       b' (vals edges)))))))

(defn assert-graph
  "Throws if broken indices or dangling refs. O(|nodes|+|edges|)."
  [g]
  (let [nodes (:nodes g) edges (:edges g)
        {:keys [incidence by-type spans]} (:ix g)]
    ;; pins reference existing nodes
    (doseq [[eid e] edges
            :let [pins (:pins e)]]
      (doseq [{:keys [node]} pins]
        (when-not (contains? nodes node)
          (throw (ex-info "Dangling pin" {:eid eid :node node})))))
    ;; incidence matches edges
    (doseq [[nid eids] incidence
            :when (not (contains? nodes nid))]
      (throw (ex-info "Incidence for missing node" {:nid nid})))
    (doseq [[eid e] edges
            :when (not-every? #(contains? (get incidence (:node %)) eid) (:pins e))]
      (throw (ex-info "Incidence mismatch" {:eid eid})))
    ;; by-type contains ids
    (doseq [[t nids] (get by-type :node)]
      (doseq [nid nids]
        (when-not (contains? nodes nid)
          (throw (ex-info "by-type node mismatch" {:type t :nid nid})))))
    (doseq [[t eids] (get by-type :edge)]
      (doseq [eid eids]
        (when-not (contains? edges eid)
          (throw (ex-info "by-type edge mismatch" {:type t :eid eid})))))
    ;; spans slots reference existing ids
    (doseq [[s {:keys [ids kind end]}] spans
            id ids]
      (case kind
        :node (when-not (contains? nodes id)
                (throw (ex-info "span refers to missing node" {:start s :id id})))
        :edge (when-not (contains? edges id)
                (throw (ex-info "span refers to missing edge" {:start s :id id})))
        (throw (ex-info "unknown span kind" {:kind kind}))))
    true))

;;; ============================================================================
;;; Examples & Cookbook
;;; ============================================================================

(comment
;;; ============================================================================
;;; 0) Setup
;;; ============================================================================

  (def g0 (empty-graph))

;;; ============================================================================
;;; 1) Basic nodes/edges (explicit & implicit IDs)
;;; ============================================================================

  ;; Implicit IDs
  (def g1 (-> g0
              (add-node {:type :person :name "Alice"})
              (add-node {:type :person :name "Bob"})
              (add-node {:type :project :name "Hypergraph"})))

  ;; Explicit ID (counter bumps so no collisions later)
  (def g1' (add-node g1 {:id 42 :type :person :name "Carol"}))
  (get-in g1' [:nodes 42]) ;; => {:id 42, :type :person, ...}

  ;; Hyperedge with roles
  (def g2 (-> g1'
              (add-edge {:type :knows
                         :pins [{:node 0 :role :knower}
                                {:node 1 :role :known}]})
              (add-edge {:type :works-on
                         :pins [{:node 0 :role :worker}
                                {:node 2 :role :project}]
                         :attrs {:hours-per-week 20}})))

  (neighbors g2 0)             ;; => [1 2]
  (neighbors g2 0 :knows nil)  ;; => [1]
  (edges-of-type g2 :works-on) ;; => #{1}  (ids will vary)

;;; ============================================================================
;;; 2) Batch build (transients) and data-driven ops
;;; ============================================================================

  ;; Builder API
  (def g3 (build g0
                 (fn [b]
                   (let [[b a] (add-node! b {:type :doc :content "Hello"})
                         [b z] (add-node! b {:type :doc :content "World"})
                         [b _] (add-edge! b {:type :link
                                             :pins [{:node a :role :from}
                                                    {:node z :role :to}]})]
                     b))))

  ;; Data-driven ops
  (def g4 (build-ops g0
                     [[:node {:type :lang :name "Clojure"}]
                      [:node {:type :lang :name "Idris2"}]
                      [:edge {:type :influences
                              :pins [{:node 0 :role :src}
                                     {:node 1 :role :dst}]}]]))
  (neighbors g4 0 :influences :dst) ;; => [1]

;;; ============================================================================
;;; 3) Removal and integrity checks
;;; ============================================================================

  ;; Remove edge
  (def e-to-drop (first (edges-of-type g2 :knows)))
  (def g5 (rem-edge g2 e-to-drop))
  (edges-of-type g5 :knows) ;; => #{}

  ;; Remove node (and all incident edges)
  (def g6 (rem-node g2 1))
  (get-in g6 [:nodes 1]) ;; => nil
  (incident-edges g6 1)  ;; => #{}

  ;; Optional: fast sanity checks during dev
  #_(assert-graph g6) ;; throws if indices are inconsistent

;;; ============================================================================
;;; 4) Spans: covering and overlaps
;;; ============================================================================

  (def text-layer
    (-> (empty-graph)
        (add-node {:type :paragraph
                   :text "The quick brown fox jumps over the lazy dog"
                   :span {:start 0 :end 44}})))

  ;; Single covering slot
  (covering text-layer 25)
  ;; => {:kind :node, :start 0, :end 44, :ids #{0}}

  ;; Overlapping spans as a separate graph/layer
  (def annotation-layer
    (-> (empty-graph)
        (add-node {:type :noun :text "fox"   :span {:start 16 :end 19}})
        (add-node {:type :verb :text "jumps" :span {:start 20 :end 25}})
        (add-node {:type :noun :text "dog"   :span {:start 41 :end 44}})))

  ;; If you added covering-all:
  #_(covering-all annotation-layer 21)
  #_;; => [{:kind :node :id 1 :start 20 :end 25}]

;;; ============================================================================
;;; 5) Traversals (BFS/DFS) with role/type filters
;;; ============================================================================

  ;; Build a tiny tree with :child edges
    (def gtree
      (build (empty-graph)
             (fn [b]
               (let [[b r] (add-node! b {:type :element :name "root"})
                     [b a] (add-node! b {:type :element :name "a"})
                     [b b1] (add-node! b {:type :element :name "b"})
                     [b c] (add-node! b {:type :element :name "c"})
                     [b _] (add-edge! b {:type :child
                                         :pins [{:node r :role :parent}
                                                {:node a :role :child :i 0}]})
                     [b _] (add-edge! b {:type :child
                                         :pins [{:node r :role :parent}
                                                {:node b1 :role :child :i 1}]})
                     [b _] (add-edge! b {:type :child
                                         :pins [{:node a :role :parent}
                                                {:node c :role :child :i 0}]})]
                 b))))

  ;; BFS over children
  (take 10 (bfs-seq gtree [0] {:edge-type :child :role :child}))
  ;; => (0 1 2 3)  ; root, then a/b, then c

  ;; DFS over children
  (take 10 (dfs-seq gtree [0] {:edge-type :child :role :child}))
  ;; => (0 1 3 2)

  ;; Streaming neighbors (seq) if you added neighbors-seq:
  #_(doall (neighbors-seq gtree 0 :child :child)) ;; => (1 2)

  ;; Ordered children helper (uses :i)
  (defn children-ordered [g nid]
    (->> (edges-of-type g :child)
         (map #(get-in g [:edges %]))
         (keep (fn [e]
                 (when (some #(and (= (:role %) :parent) (= (:node %) nid))
                             (:pins e))
                   (some #(when (= (:role %) :child) %) (:pins e)))))
         (sort-by :i)
         (mapv :node)))

  (children-ordered gtree 0) ;; => [1 2]
  (children-ordered gtree 1) ;; => [3]

;;; ============================================================================
;;; 6) Layers & composition
;;; ============================================================================

  (def composite (compose-layers {:text text-layer
                                  :annotations annotation-layer}))
  (:node-ids composite)   ;; => {:text 0, :annotations 1} (ids vary)
  (:meta-graph composite) ;; => graph of layer-nodes

;;; ============================================================================
;;; 7) Merge graphs (ID-offset strategy) + reindex
;;; ============================================================================

  (def merged (merge-graphs g2 g3)) ;; offsets g3 into g2's id space and reindexes
  (nodes-of-type merged :person) ;; ids from g2; doc nodes from g3 also present
  (edges-of-type merged :link)   ;; => from g3, remapped

;;; ============================================================================
;;; 8) Subgraph extraction
;;; ============================================================================

  ;; keep only Alice + Hypergraph project and connecting edges
  (def sg (subgraph g2 [0 2]))
  (keys (:nodes sg))           ;; => (0 2)
  (edges-of-type sg :works-on) ;; => #{...} if both endpoints kept

;;; ============================================================================
;;; 9) Round-trip editing pattern (span-driven; sketch)
;;; ============================================================================

  ;; Find node covering position and compute patch range
  (let [{:keys [ids]} (covering text-layer 21)
        nid (first ids)
        {:keys [span]} (get-in text-layer [:nodes nid])]
    ;; Use span {:start s :end e} to splice bytes/text externally.
    ;; Then update node span if needed with (rem-node + add-node) or a dedicated update.
    [nid span])

;;; ============================================================================
;;; 10) Diffing two graphs (cheap structural)
;;; ============================================================================

  (defn diff-ids [a b]
    {:added   (set/difference (set (keys b)) (set (keys a)))
     :removed (set/difference (set (keys a)) (set (keys b)))
     :common  (set/intersection (set (keys a)) (set (keys b)))})

  (defn changed-nodes [gA gB]
    (let [{:keys [common]} (diff-ids (:nodes gA) (:nodes gB))]
      (->> common
           (filter (fn [nid]
                     (not= (dissoc (get-in gA [:nodes nid]) :span) ;; ignore span changes if you want
                           (dissoc (get-in gB [:nodes nid]) :span))))
           set)))

  (defn changed-edges [gA gB]
    (let [{:keys [common]} (diff-ids (:edges gA) (:edges gB))]
      (->> common
           (filter #(not= (get-in gA [:edges %])
                          (get-in gB [:edges %])))
           set)))

  (changed-nodes g1 g1') ;; => #{42} (Carol new)
  (changed-edges g2 g5)  ;; => #{<eid-of-knows>} (removed)

;;; ============================================================================
;;; 11) Path queries (quick-n-dirty)
;;; ============================================================================

  ;; all (worker -> project) pairs
  (def worker->project
    (for [eid (edges-of-type g2 :works-on)
          :let [e (get-in g2 [:edges eid])
                w (some #(when (= (:role %) :worker) (:node %)) (:pins e))
                p (some #(when (= (:role %) :project) (:node %)) (:pins e))]
          :when (and w p)]
      [w p]))
  worker->project ;; => [[0 2]]

;;; ============================================================================
;;; 12) Reindex from scratch (after manual merges/imports)
;;; ============================================================================

  (def g2-re (reindex g2))
  (= (:ix g2) (:ix g2-re)) ;; => true

;;; ============================================================================
;;; 13) Streaming traversal with transducers (BFS as source)
;;; ============================================================================

  ;; If you wired neighbors-seq / queue BFS, you can do:
  (transduce (comp (filter #(= "Alice" (get-in g2 [:nodes % :name])))
                   (map #(vector % (get-in g2 [:nodes %]))))
             conj
             (bfs-seq g2 [0])) ;; => [[0 {:name "Alice" ...}]]

  ;; Or plain sequence pipelines:
  (->> (bfs-seq gtree [0] {:edge-type :child :role :child})
       (map #(get-in gtree [:nodes % :name]))
       (take 10)))
