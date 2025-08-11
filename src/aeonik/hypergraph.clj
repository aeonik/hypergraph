(ns aeonik.hypergraph
  (:require [clojure.set :as set]))

(defn empty-graph []
  {:nodes {}
   :edges {}
   :meta  {:next-nid 0 :next-eid 0}
   :ix    {:incidence {}
           :by-type   {}
           :spans     (sorted-map)}})

(defn- alloc [g k]
  (let [id (get-in g [:meta k])]
    [id (assoc-in g [:meta k] (inc id))]))

(defn- index-span [g kind id {:keys [start end]}]
  (if (and start end)
    (update-in g [:ix :spans start]
               (fnil (fn [slot] (-> slot
                                    (update :ids (fnil conj #{}) id)
                                    (assoc  :end end :kind kind)))
                     {:end end :ids #{id} :kind kind}))
    g))

(defn- deindex-span [g kind id {:keys [start]}]
  (if-let [slot (get-in g [:ix :spans start])]
    (let [ids (disj (:ids slot) id)]
      (if (seq ids)
        (assoc-in g [:ix :spans start] (assoc slot :ids ids))
        (update-in g [:ix :spans] dissoc start)))
    g))

(defn add-node
  [g {:keys [id type span] :as node}]
  (let [[id g] (if (some? id) [id g] (alloc g :next-nid))
        node   (assoc node :id id)]
    (-> g
        (assoc-in  [:nodes id] node)
        (update-in [:ix :by-type :node type] (fnil conj #{}) id)
        (index-span :node id span))))   ;; <— drop explicit g

(defn add-edge
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
              g
              pins)
      (index-span g :edge id span))))
   ;; <— drop explicit g

(defn rem-edge [g eid]
  (let [edge (get-in g [:edges eid])]
    (when-not edge (throw (ex-info "No such edge" {:eid eid})))
    (as-> g g
      (update-in g [:ix :by-type :edge (:type edge)] (fnil disj #{}) eid)
      (reduce (fn [g {:keys [node]}]
                (update-in g [:ix :incidence node] disj eid))
              g
              (:pins edge))
      (deindex-span g :edge eid (:span edge))
      (update g :edges dissoc eid))))

(defn rem-node [g nid]
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

(defn incident-edges [g nid] (get-in g [:ix :incidence nid] #{}))

(defn neighbors
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

(defn covering [g x]
  (let [m   (get-in g [:ix :spans])
        a   (subseq m <= x)
        hit (last a)]
    (when hit
      (let [[start {:keys [end ids kind]}] hit]
        (when (< x end) {:kind kind :start start :end end :ids ids})))))

(defn attach-element
  [g {:keys [name span attrs children]} parent-nid k-idx]
  (let [[nid g1] (alloc g :next-nid)
        g2       (add-node g1 {:id nid :type :element :name name :span span :attrs attrs})
        g3       (if parent-nid
                   (add-edge g2 {:type :child
                                 :pins [{:node parent-nid :role :parent}
                                        {:node nid        :role :child  :i k-idx}]})
                   g2)]
    (loop [g g3, i 0, cs children]
      (if (seq cs)
        (recur (attach-element g (first cs) nid i) (inc i) (rest cs))
        g))))

(defn bfs-seq
  "Lazy BFS over node ids. opts: {:edge-type kw, :role kw} to constrain neighbor set."
  ([g roots] (bfs-seq g roots {}))
  ([g roots {:keys [edge-type role]}]
   (letfn [(nbrs [nid]
             ;; make neighbors a *seq* (not vec) to keep laziness downstream
             (let [eids (get-in g [:ix :incidence nid] #{})]
               (mapcat (fn [eid]
                         (let [e (get-in g [:edges eid])]
                           (when (or (nil? edge-type) (= (:type e) edge-type))
                             (for [p (:pins e)
                                   :let [m (:node p)]
                                   :when (and (not= m nid)
                                              (or (nil? role) (= role (:role p))))]
                               m))))
                       eids)))]
     (letfn [(step [q seen]
               (lazy-seq
                (when-let [q (seq q)]
                  (let [nid (first q)
                        more (rest q)]
                    (cons nid
                          (let [ns (remove seen (nbrs nid))]
                            (step (concat more ns) (into seen ns))))))))]
       (step (distinct roots) (set roots))))))
(defrecord Builder
           [nodes* edges*
            inc*                     ; transient map: nid -> #{eid ...} (values are persistent sets; OK)
            btn* bte*                ; transient maps: by-type for nodes/edges (values persistent sets)
            spans-base spans*        ; spans-base: existing sorted-map; spans*: transient unsorted map (k->slot)
            next-nid next-eid])      ; counters carried here (ints)

(defn begin [g]
  {:nodes* (transient (:nodes g))
   :edges* (transient (:edges g))
   :inc*   (transient (get-in g [:ix :incidence] {}))
   :btn*   (transient (get-in g [:ix :by-type :node] {}))
   :bte*   (transient (get-in g [:ix :by-type :edge] {}))
   :spans-base (get-in g [:ix :spans] (sorted-map))
   :spans* (transient {})
   :next-nid (get-in g [:meta :next-nid] 0)
   :next-eid (get-in g [:meta :next-eid] 0)})

(defn- spans-upsert!
  "Record a span for {kind,id} at {:start s :end e} into builder map."
  [b kind id {:keys [start end]}]
  (if (and start end)
    (let [sb   (:spans-base b)
          sm   (:spans* b)                  ; transient map
          slot (or (get sm start)
                   (get sb start)
                   {:end end :ids #{} :kind kind})
          slot' (-> slot
                    (update :ids conj id)
                    (assoc  :end end :kind kind))
          sm'  (assoc! sm start slot')]
      (assoc b :spans* sm'))                ; put the new transient back
    b))

(defn add-node!
  "Transient-friendly: mutate builder map’s transient fields."
  [b {:keys [id type span] :as node}]
  (let [provided? (some? id)
        id        (long (if provided? id (:next-nid b)))
        node      (assoc node :id id)
        ;; nodes*
        nodes*    (assoc! (:nodes* b) id node)
        ;; btn* (by-type for nodes). Values stay persistent sets.
        btn*      (let [m (:btn* b)
                        s (get m type #{})]
                    (assoc! m type (conj s id)))
        b         (-> b
                      (assoc :nodes* nodes*)
                      (assoc :btn*   btn*)
                      (update :next-nid #(if provided? % (inc id))))]
    (spans-upsert! b :node id span)
    [b id]))

(defn add-edge!
  "Transient-friendly hyperedge add."
  [b {:keys [id type pins attrs span] :as edge}]
  ;; sanity: all pin nodes must already exist in nodes*
  (doseq [{:keys [node]} pins]
    (when-not (contains? (:nodes* b) node)
      (throw (ex-info "Pin references missing node" {:edge id :pin node}))))
  (let [provided? (some? id)
        id        (long (if provided? id (:next-eid b)))
        edge'     (-> edge (assoc :id id :attrs (or attrs {}) :pins (vec pins)))
        ;; edges*
        edges*    (assoc! (:edges* b) id edge')
        ;; bte* (by-type for edges)
        bte*      (let [m (:bte* b)
                        s (get m type #{})]
                    (assoc! m type (conj s id)))
        ;; incidence
        inc*      (reduce (fn [m {:keys [node]}]
                            (assoc! m node (conj (get m node #{}) id)))
                          (:inc* b) pins)
        b         (-> b
                      (assoc :edges* edges*)
                      (assoc :bte*   bte*)
                      (assoc :inc*   inc*)
                      (update :next-eid #(if provided? % (inc id))))]
    (spans-upsert! b :edge id span)
    [b id]))

(defn finish
  "Close the builder, returning a persistent graph."
  [^Builder b]
  (let [nodes (persistent! (:nodes* b))
        edges (persistent! (:edges* b))
        inc   (persistent! (:inc* b))
        btn   (persistent! (:btn* b))
        bte   (persistent! (:bte* b))
        ;; merge spans: base + new, then re-sort
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

(defn add-nodes-batch
  "nodes-coll is a seq of node maps (w/o :id). Returns [graph ids-vector]."
  [g nodes-coll]
  (let [b0 (begin g)
        [b ids] (reduce (fn [[b acc] n]
                          (let [[b id] (add-node! b n)]
                            [b (conj acc id)]))
                        [b0 []]
                        nodes-coll)]
    [(finish b) ids]))

(defn add-edges-batch
  "edges-coll is a seq of edge maps. Returns graph."
  [g edges-coll]
  (let [b0 (begin g)
        b  (reduce (fn [b e] (first (add-edge! b e))) b0 edges-coll)]
    (finish b)))

(comment
  (def g0 (empty-graph))
  ;; => #'aeonik.hypergraph/g0

  (def g1 (add-node g0 {:type :element :name "ActionMaps" :span {:start 0 :end 200}}))
  ;; => #'aeonik.hypergraph/g1

  (def g2 (add-node g1 {:type :element :name "actionmap"  :span {:start 20 :end 120}}))
  ;; => #'aeonik.hypergraph/g2

  (def g3 (add-edge g2 {:type :child
                        :pins [{:node 0 :role :parent}
                               {:node 1 :role :child :i 0}]}))

  (def g-batch (let [b0   (begin g0)
                     [b1 r] (add-node! b0 {:type :element :name "ActionMaps" :span {:start 0 :end 200}})
                     [b2 c] (add-node! b1 {:type :element :name "actionmap"  :span {:start 20 :end 120}})
                     [b3 _] (add-edge! b2 {:type :child
                                           :pins [{:node r :role :parent}
                                                  {:node c :role :child :i 0}]})
                     g'    (finish b3)]
                 g'))

;; example shapes (don’t leave raw forms at top-level)
  #_{:id 42 :type :attr :k "name" :v "player" :span {:start 123 :end 130} :owner ::parent})
