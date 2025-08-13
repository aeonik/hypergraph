(ns aeonik.multi-hypergraph-test
  (:require [clojure.test :refer :all]
            [aeonik.multi-hypergraph :as mhg]))

;;; ============================================================================
;;; Core Data Structure Tests
;;; ============================================================================

(deftest test-empty-graph
  (testing "Empty graph structure"
    (let [g (mhg/empty-graph)]
      (is (= {} (:nodes g)))
      (is (= {} (:edges g)))
      (is (= 0 (get-in g [:meta :next-nid])))
      (is (= 0 (get-in g [:meta :next-eid])))
      (is (= {} (get-in g [:ix :incidence])))
      (is (= {} (get-in g [:ix :by-type])))
      (is (sorted? (get-in g [:ix :spans]))))))

;;; ============================================================================
;;; Node Operations Tests
;;; ============================================================================

(deftest test-add-node
  (testing "Adding nodes with auto-generated IDs"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice"})
                (mhg/add-node {:type :person :name "Bob"}))]
      (is (= 2 (count (:nodes g))))
      (is (= "Alice" (get-in g [:nodes 0 :name])))
      (is (= "Bob" (get-in g [:nodes 1 :name])))
      (is (= #{0 1} (mhg/nodes-of-type g :person)))))

  (testing "Adding node with explicit ID"
    (let [g (mhg/add-node (mhg/empty-graph)
                          {:id 42 :type :special :name "Explicit"})]
      (is (= 42 (get-in g [:nodes 42 :id])))
      (is (= 43 (get-in g [:meta :next-nid])))
      (is (= #{42} (mhg/nodes-of-type g :special)))))

  (testing "Adding node with span"
    (let [g (mhg/add-node (mhg/empty-graph)
                          {:type :text :content "Hello"
                           :span {:start 0 :end 5}})]
      (is (= {:start 0 :end 5} (get-in g [:nodes 0 :span])))
      (is (contains? (get-in g [:ix :spans]) 0))
      (is (= #{0} (get-in g [:ix :spans 0 :ids]))))))

(deftest test-rem-node
  (testing "Removing a node"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice"})
                (mhg/add-node {:type :person :name "Bob"}))
          g' (mhg/rem-node g 0)]
      (is (= 1 (count (:nodes g'))))
      (is (nil? (get-in g' [:nodes 0])))
      (is (= #{1} (mhg/nodes-of-type g' :person)))))

  (testing "Removing non-existent node throws"
    (is (thrown? Exception
                 (mhg/rem-node (mhg/empty-graph) 999)))))

;;; ============================================================================
;;; Edge Operations Tests
;;; ============================================================================

(deftest test-add-edge
  (testing "Adding basic edge"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice"})
                (mhg/add-node {:type :person :name "Bob"})
                (mhg/add-edge {:type :knows
                               :pins [{:node 0 :role :knower}
                                      {:node 1 :role :known}]}))]
      (is (= 1 (count (:edges g))))
      (is (= :knows (get-in g [:edges 0 :type])))
      (is (= #{0} (mhg/incident-edges g 0)))
      (is (= #{0} (mhg/incident-edges g 1)))
      (is (= #{0} (mhg/edges-of-type g :knows)))))

  (testing "Adding edge with attributes"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person})
                (mhg/add-node {:type :project})
                (mhg/add-edge {:type :works-on
                               :pins [{:node 0 :role :worker}
                                      {:node 1 :role :project}]
                               :attrs {:hours 40}}))]
      (is (= {:hours 40} (get-in g [:edges 0 :attrs])))))

  (testing "Adding edge with invalid node reference throws"
    (let [g (mhg/add-node (mhg/empty-graph) {:type :person})]
      (is (thrown? Exception
                   (mhg/add-edge g {:type :knows
                                    :pins [{:node 0 :role :knower}
                                           {:node 999 :role :known}]})))))

  (testing "Hyperedge with multiple nodes"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice"})
                (mhg/add-node {:type :person :name "Bob"})
                (mhg/add-node {:type :person :name "Carol"})
                (mhg/add-edge {:type :meeting
                               :pins [{:node 0 :role :participant}
                                      {:node 1 :role :participant}
                                      {:node 2 :role :participant}]}))]
      (is (= 3 (count (get-in g [:edges 0 :pins]))))
      (is (= #{0} (mhg/incident-edges g 0)))
      (is (= #{0} (mhg/incident-edges g 1)))
      (is (= #{0} (mhg/incident-edges g 2))))))

(deftest test-rem-edge
  (testing "Removing an edge"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person})
                (mhg/add-node {:type :person})
                (mhg/add-edge {:type :knows
                               :pins [{:node 0 :role :knower}
                                      {:node 1 :role :known}]}))
          g' (mhg/rem-edge g 0)]
      (is (= 0 (count (:edges g'))))
      (is (= #{} (mhg/incident-edges g' 0)))
      (is (= #{} (mhg/edges-of-type g' :knows)))))

  (testing "Removing non-existent edge throws"
    (is (thrown? Exception
                 (mhg/rem-edge (mhg/empty-graph) 999)))))

(deftest test-rem-node-cascades
  (testing "Removing node removes incident edges"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice"})
                (mhg/add-node {:type :person :name "Bob"})
                (mhg/add-node {:type :person :name "Carol"})
                (mhg/add-edge {:type :knows
                               :pins [{:node 0 :role :knower}
                                      {:node 1 :role :known}]})
                (mhg/add-edge {:type :knows
                               :pins [{:node 0 :role :knower}
                                      {:node 2 :role :known}]}))
          g' (mhg/rem-node g 0)]
      (is (= 2 (count (:nodes g'))))
      (is (= 0 (count (:edges g'))))
      (is (= #{} (mhg/edges-of-type g' :knows))))))

;;; ============================================================================
;;; Query Tests
;;; ============================================================================

(deftest test-neighbors
  (testing "Finding all neighbors"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person})
                (mhg/add-node {:type :person})
                (mhg/add-node {:type :person})
                (mhg/add-edge {:type :knows
                               :pins [{:node 0 :role :knower}
                                      {:node 1 :role :known}]})
                (mhg/add-edge {:type :works-with
                               :pins [{:node 0 :role :colleague}
                                      {:node 2 :role :colleague}]}))]
      (is (= #{1 2} (set (mhg/neighbors g 0))))
      (is (= [1] (mhg/neighbors g 0 :knows nil)))
      (is (= [2] (mhg/neighbors g 0 :works-with nil)))))

  (testing "Filtering by role"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :element})
                (mhg/add-node {:type :element})
                (mhg/add-node {:type :element})
                (mhg/add-edge {:type :child
                               :pins [{:node 0 :role :parent}
                                      {:node 1 :role :child}]})
                (mhg/add-edge {:type :child
                               :pins [{:node 1 :role :parent}
                                      {:node 2 :role :child}]}))]
      (is (= [1] (mhg/neighbors g 0 :child :child)))
      (is (= [0] (mhg/neighbors g 1 :child :parent)))
      (is (= [2] (mhg/neighbors g 1 :child :child))))))

(deftest test-find-id
  (testing "Finding node by predicate"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice" :age 30})
                (mhg/add-node {:type :person :name "Bob" :age 25}))]
      (is (= 0 (mhg/find-id g #(= "Alice" (:name %)))))
      (is (= 1 (mhg/find-id g #(= 25 (:age %)))))
      (is (nil? (mhg/find-id g #(= "Carol" (:name %))))))))

;;; ============================================================================
;;; Span Tests
;;; ============================================================================

(deftest test-covering
  (testing "Finding covering span"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :paragraph
                               :span {:start 0 :end 100}})
                (mhg/add-node {:type :word
                               :span {:start 10 :end 20}}))
          result (mhg/covering g 50)]
                                        ; First check if we're getting anything at all
      (is (not (nil? result)) "Should find a covering span at position 50")
      (when result
        (is (= :node (:kind result)))
        (is (= 0 (:start result)))
        (is (= 100 (:end result)))
        (is (= #{0} (:ids result)))))

    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :word
                               :span {:start 10 :end 20}}))
          result (mhg/covering g 15)]
      (is (not (nil? result)) "Should find a covering span at position 15")
      (when result
        (is (= :node (:kind result)))
        (is (= 10 (:start result)))
        (is (= 20 (:end result)))
        (is (= #{0} (:ids result)))))

    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :paragraph
                               :span {:start 0 :end 100}}))
          result (mhg/covering g 200)]
      (is (nil? result) "Should not find anything at position 200")))

  (testing "Multiple elements at same position"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :text :span {:start 0 :end 10}})
                (mhg/add-node {:type :highlight :span {:start 0 :end 10}}))]
      (let [result (mhg/covering g 5)]
        (is (= 0 (:start result)))
        (is (= 10 (:end result)))
        (is (= #{0 1} (:ids result)))))))

(deftest test-covering-all
  (testing "Finding all covering spans"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :doc :span {:start 0 :end 100}})
                (mhg/add-node {:type :section :span {:start 20 :end 80}})
                (mhg/add-node {:type :paragraph :span {:start 30 :end 50}}))]
      (let [results (mhg/covering-all g 40)]
        (is (= 3 (count results)))
        (is (every? #(= :node (:kind %)) results))
        (is (= #{0 1 2} (set (map :id results))))))))

;;; ============================================================================
;;; Traversal Tests
;;; ============================================================================

(deftest test-bfs-seq
  (testing "Breadth-first traversal (undirected)"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :node :name "root"}) ; 0
                (mhg/add-node {:type :node :name "a"})    ; 1
                (mhg/add-node {:type :node :name "b"})    ; 2
                (mhg/add-node {:type :node :name "c"})    ; 3
                (mhg/add-node {:type :node :name "d"})    ; 4
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 1 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 2 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 1 :role :from}
                                      {:node 3 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 2 :role :from}
                                      {:node 4 :role :to}]}))]
      (is (= [0 1 2 3 4] (vec (mhg/bfs-seq g [0]))))
      (is (= [0 1 2] (vec (take 3 (mhg/bfs-seq g [0])))))
      (is (= [1 0 3] (vec (take 3 (mhg/bfs-seq g [1])))))))

  (testing "Breadth-first traversal (directed: from->to)"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :node :name "root"}) ; 0
                (mhg/add-node {:type :node :name "a"})    ; 1
                (mhg/add-node {:type :node :name "b"})    ; 2
                (mhg/add-node {:type :node :name "c"})    ; 3
                (mhg/add-node {:type :node :name "d"})    ; 4
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 1 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 2 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 1 :role :from}
                                      {:node 3 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 2 :role :from}
                                      {:node 4 :role :to}]}))]
      (is (= [0 1 2 3 4]
             (vec (mhg/bfs-seq g [0] {:directed? true :from-role :from :to-role :to}))))
      (is (= [0 1 2]
             (vec (take 3 (mhg/bfs-seq g [0] {:directed? true :from-role :from :to-role :to})))))
      (is (= [1 3]
             (vec (mhg/bfs-seq g [1] {:directed? true :from-role :from :to-role :to}))))
      (is (= [2 4]
             (vec (mhg/bfs-seq g [2] {:directed? true :from-role :from :to-role :to}))))
      (is (= [3]
             (vec (mhg/bfs-seq g [3] {:directed? true :from-role :from :to-role :to}))))
      (is (= [4]
             (vec (mhg/bfs-seq g [4] {:directed? true :from-role :from :to-role :to}))))
      (is (= [1 0]
             (vec (mhg/bfs-seq g [1]
                               {:directed? true :from-role :to :to-role :from}))))

      (is (= [3 1 0]
             (vec (mhg/bfs-seq g [3]
                               {:directed? true :from-role :to :to-role :from}))))
      (is (= [1]
             (vec (take 1 (mhg/bfs-seq g [1]
                                       {:directed? true :from-role :to :to-role :from})))))
      (is (= [3]
             (vec (take 1 (mhg/bfs-seq g [3]
                                       {:directed? true :from-role :to :to-role :from})))))))

  (testing "Breadth-first traversal (directed: to->from, should traverse reversed edge)"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :node :name "root"}) ; 0
                (mhg/add-node {:type :node :name "a"})    ; 1
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 1 :role :to}]}))]
      (is (= [1 0]
             (vec (mhg/bfs-seq g [1] {:directed? true :from-role :to :to-role :from}))))
      (is (= [0 1]
             (vec (mhg/bfs-seq g [0] {:directed? true :from-role :from :to-role :to})))))))

(deftest test-dfs-seq
  (testing "Depth-first traversal"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :node :name "root"})    ; 0
                (mhg/add-node {:type :node :name "a"})       ; 1
                (mhg/add-node {:type :node :name "b"})       ; 2
                (mhg/add-node {:type :node :name "c"})       ; 3
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 1 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 2 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 1 :role :from}
                                      {:node 3 :role :to}]}))]
      (let [result (vec (mhg/dfs-seq g [0]))]
        (is (= 4 (count result)))
        (is (= 0 (first result)))
        (is (contains? #{[0 2 1 3] [0 1 3 2]} result))))))

;;; ============================================================================
;;; Batch Operations Tests
;;; ============================================================================

(deftest test-batch-operations
  (testing "Basic batch building"
    (let [g (mhg/build (mhg/empty-graph)
                       (fn [b]
                         (let [[b n1] (mhg/add-node! b {:type :person :name "Alice"})
                               [b n2] (mhg/add-node! b {:type :person :name "Bob"})
                               [b _]  (mhg/add-edge! b {:type :knows
                                                        :pins [{:node n1 :role :knower}
                                                               {:node n2 :role :known}]})]
                           b)))]
      (is (= 2 (count (:nodes g))))
      (is (= 1 (count (:edges g))))
      (is (= "Alice" (get-in g [:nodes 0 :name])))
      (is (= "Bob" (get-in g [:nodes 1 :name])))))

  (testing "Batch with explicit IDs"
    (let [g (mhg/build (mhg/empty-graph)
                       (fn [b]
                         (let [[b _] (mhg/add-node! b {:id 100 :type :special})
                               [b _] (mhg/add-node! b {:id 200 :type :special})]
                           b)))]
      (is (contains? (:nodes g) 100))
      (is (contains? (:nodes g) 200))
      (is (>= (get-in g [:meta :next-nid]) 201)))))

;;; ============================================================================
;;; DSL Tests
;;; ============================================================================

(deftest test-dsl-basic
  (testing "Basic DSL operations"
    (let [g (-> (mhg/begin* (mhg/empty-graph))
                (mhg/node :alice {:type :person :name "Alice"})
                (mhg/node :bob {:type :person :name "Bob"})
                (mhg/edge :knows [[:alice :knower] [:bob :known]])
                mhg/finish*)]
      (is (= 2 (count (:nodes g))))
      (is (= 1 (count (:edges g))))
      (is (= "Alice" (get-in g [:nodes 0 :name])))
      (is (= "Bob" (get-in g [:nodes 1 :name])))))

  (testing "DSL with helper functions"
    (let [g (-> (mhg/begin* (mhg/empty-graph))
                (mhg/node :parent {:type :element :name "div"})
                (mhg/node :child1 {:type :element :name "span"})
                (mhg/node :child2 {:type :element :name "p"})
                (mhg/child :parent :child1 0)
                (mhg/child :parent :child2 1)
                mhg/finish*)]
      (is (= 3 (count (:nodes g))))
      (is (= 2 (count (:edges g))))
      (is (is (= #{0 1} (mhg/edges-of-type g :child))))))

  (testing "DSL with social graph helpers"
    (let [g (-> (mhg/begin* (mhg/empty-graph))
                (mhg/node :alice {:type :person :name "Alice"})
                (mhg/node :bob {:type :person :name "Bob"})
                (mhg/node :team {:type :team :name "Engineering"})
                (mhg/knows :alice :bob)
                (mhg/member-of :alice :team)
                (mhg/member-of :bob :team)
                mhg/finish*)]
      (is (= 3 (count (:nodes g))))
      (is (= 3 (count (:edges g))))
      (is (= 1 (count (mhg/edges-of-type g :knows))))
      (is (= 2 (count (mhg/edges-of-type g :member-of)))))))

(deftest test-dsl-continuation
  (testing "Continuing to build on existing graph"
    (let [initial (-> (mhg/begin* (mhg/empty-graph))
                      (mhg/node :alice {:type :person :name "Alice"})
                      (mhg/node :bob {:type :person :name "Bob"})
                      (mhg/knows :alice :bob)
                      mhg/finish*)
          extended (-> (mhg/continue* initial {:alice 0 :bob 1})
                       (mhg/node :carol {:type :person :name "Carol"})
                       (mhg/knows :bob :carol)
                       (mhg/knows :carol :alice)
                       mhg/finish*)]
      (is (= 3 (count (:nodes extended))))
      (is (= 3 (count (:edges extended))))
      (is (= "Carol" (get-in extended [:nodes 2 :name])))))

  (testing "Continue with auto-discovered aliases"
    (let [initial (-> (mhg/begin* (mhg/empty-graph))
                      (mhg/node :a {:type :person :name "Alice"})
                      (mhg/node :b {:type :person :name "Bob"})
                      mhg/finish*)
          aliases (mhg/find-aliases initial :person [:name])
          extended (-> (mhg/continue* initial aliases)
                       (mhg/node :carol {:type :person :name "Carol"})
                       (mhg/edge :knows [[(aliases :alice) :knower]
                                         [:carol :known]])
                       mhg/finish*)]
      (is (contains? aliases :alice))
      (is (contains? aliases :bob))
      (is (= 3 (count (:nodes extended))))
      (is (= 1 (count (:edges extended))))))

  (testing "Continue with mixed IDs and aliases"
    (let [initial (-> (mhg/begin* (mhg/empty-graph))
                      (mhg/node :x {:type :person :name "Alice"})
                      (mhg/node :y {:type :person :name "Bob"})
                      mhg/finish*)
          extended (-> (mhg/continue* initial)
                       (mhg/node :carol {:type :person :name "Carol"})
                       (mhg/edge :knows [[0 :knower] [:carol :known]])
                       (mhg/edge :knows [[:carol :knower] [1 :known]])
                       mhg/finish*)]
      (is (= 3 (count (:nodes extended))))
      (is (= 2 (count (:edges extended))))))

  (testing "Streaming pattern"
    (let [add-person (fn [g name]
                       (-> (mhg/continue* g)
                           (mhg/node (keyword (clojure.string/lower-case name))
                                     {:type :person :name name})
                           mhg/finish*))
          g (reduce add-person
                    (mhg/empty-graph)
                    ["Alice" "Bob" "Carol"])]
      (is (= 3 (count (:nodes g))))
      (is (= #{"Alice" "Bob" "Carol"}
             (set (map :name (vals (:nodes g))))))))

  (testing "With-aliases helper"
    (let [initial (-> (mhg/begin* (mhg/empty-graph))
                      (mhg/node :x {:type :person :name "Alice"})
                      (mhg/node :y {:type :person :name "Bob"})
                      mhg/finish*)
          extended (-> (mhg/continue* initial)
                       (mhg/with-aliases {:alice 0 :bob 1})
                       (mhg/edge :knows [[:alice :knower] [:bob :known]])
                       mhg/finish*)]
      (is (= 1 (count (:edges extended)))))))

(deftest test-find-aliases
  (testing "Finding aliases for nodes"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person :name "Alice Smith"})
                (mhg/add-node {:type :person :name "Bob Jones"})
                (mhg/add-node {:type :team :name "Engineering"}))
          person-aliases (mhg/find-aliases g :person [:name])
          team-aliases (mhg/find-aliases g :team [:name] "team-")]
      (is (= 0 (person-aliases :alice-smith)))
      (is (= 1 (person-aliases :bob-jones)))
      (is (= 2 (team-aliases :team-engineering)))
      (is (= 2 (count person-aliases)))
      (is (= 1 (count team-aliases))))))

(deftest test-dsl-edge-with-attrs
  (testing "DSL edge with attributes"
    (let [g (-> (mhg/begin* (mhg/empty-graph))
                (mhg/node :a {:type :node})
                (mhg/node :b {:type :node})
                (mhg/edge :connected [[:a :from] [:b :to]] {:weight 5})
                mhg/finish*)]
      (is (= {:weight 5} (get-in g [:edges 0 :attrs]))))))

(deftest test-dsl-error-handling
  (testing "Unbound alias throws"
    (is (thrown-with-msg? Exception #"Alias not bound"
                          (-> (mhg/begin* (mhg/empty-graph))
                              (mhg/node :alice {:type :person})
                              (mhg/edge :knows [[:alice :knower] [:bob :known]])
                              mhg/finish*)))))

;;; ============================================================================
;;; Graph Composition Tests
;;; ============================================================================

(deftest test-compose-layers
  (testing "Layer composition"
    (let [text-layer (-> (mhg/empty-graph)
                         (mhg/add-node {:type :text :content "Hello"}))
          format-layer (-> (mhg/empty-graph)
                           (mhg/add-node {:type :bold :span {:start 0 :end 5}}))
          composite (mhg/compose-layers {:text text-layer
                                         :format format-layer})]
      (is (contains? (:node-ids composite) :text))
      (is (contains? (:node-ids composite) :format))
      (is (= 2 (count (get-in composite [:meta-graph :nodes])))))))

(deftest test-subgraph
  (testing "Extracting subgraph"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :node :name "a"})  ; 0
                (mhg/add-node {:type :node :name "b"})  ; 1
                (mhg/add-node {:type :node :name "c"})  ; 2
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 1 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 1 :role :from}
                                      {:node 2 :role :to}]})
                (mhg/add-edge {:type :edge
                               :pins [{:node 0 :role :from}
                                      {:node 2 :role :to}]}))
          sub (mhg/subgraph g [0 1])]
      (is (= 2 (count (:nodes sub))))
      (is (= 1 (count (:edges sub))))  ; Only edge between 0 and 1
      (is (contains? (:nodes sub) 0))
      (is (contains? (:nodes sub) 1))
      (is (not (contains? (:nodes sub) 2))))))

(deftest test-merge-graphs
  (testing "Merging graphs with offset"
    (let [g1 (-> (mhg/empty-graph)
                 (mhg/add-node {:type :person :name "Alice"})
                 (mhg/add-node {:type :person :name "Bob"}))
          g2 (-> (mhg/empty-graph)
                 (mhg/add-node {:type :person :name "Carol"})
                 (mhg/add-node {:type :person :name "David"}))
          merged (mhg/merge-graphs g1 g2)]
      (is (= 4 (count (:nodes merged))))
      (is (= #{"Alice" "Bob" "Carol" "David"}
             (set (map :name (vals (:nodes merged)))))))))

;;; ============================================================================
;;; Integrity Tests
;;; ============================================================================

(deftest test-assert-graph
  (testing "Valid graph passes assertion"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person})
                (mhg/add-node {:type :person})
                (mhg/add-edge {:type :knows
                               :pins [{:node 0 :role :from}
                                      {:node 1 :role :to}]}))]
      (is (mhg/assert-graph g))))

  (testing "Graph with manual corruption fails"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :person}))
          ; Manually corrupt the index
          corrupted (assoc-in g [:ix :incidence 999] #{888})]
      (is (thrown-with-msg? Exception #"Incidence for missing node"
                            (mhg/assert-graph corrupted))))))

;;; ============================================================================
;;; Edge Case Tests
;;; ============================================================================

(deftest test-empty-graph-operations
  (testing "Operations on empty graph"
    (let [g (mhg/empty-graph)]
      (is (= #{} (mhg/incident-edges g 0)))
      (is (= [] (mhg/neighbors g 0)))
      (is (= #{} (mhg/edges-of-type g :any)))
      (is (= #{} (mhg/nodes-of-type g :any)))
      (is (nil? (mhg/covering g 0)))
      (is (= [] (mhg/covering-all g 0)))
      (is (= [] (vec (mhg/bfs-seq g [0]))))
      (is (= [] (vec (mhg/dfs-seq g [0])))))))

(deftest test-self-loops
  (testing "Edge connecting node to itself"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :state :name "recursive"})
                (mhg/add-edge {:type :self-ref
                               :pins [{:node 0 :role :from}
                                      {:node 0 :role :to}]}))]
      (is (= #{0} (mhg/incident-edges g 0)))
      ; Node 0 should not appear in its own neighbors
      (is (= [] (mhg/neighbors g 0))))))

(deftest test-ordering-pins
  (testing "Pins with ordering indices"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :parent})
                (mhg/add-node {:type :child :name "first"})
                (mhg/add-node {:type :child :name "second"})
                (mhg/add-node {:type :child :name "third"})
                (mhg/add-edge {:type :children
                               :pins [{:node 0 :role :parent}
                                      {:node 3 :role :child :i 2}
                                      {:node 1 :role :child :i 0}
                                      {:node 2 :role :child :i 1}]}))]
      (let [edge (get-in g [:edges 0])
            child-pins (->> (:pins edge)
                            (filter #(= :child (:role %)))
                            (sort-by :i)
                            (mapv :node))]
        (is (= [1 2 3] child-pins))))))

;;; ============================================================================
;;; Performance/Stress Tests (optional, comment out for regular test runs)
;;; ============================================================================

(deftest ^:performance test-large-graph
  (testing "Building graph with many nodes and edges"
    (let [node-count 1000
          g (mhg/build (mhg/empty-graph)
                       (fn [b]
                         (loop [b b, i 0]
                           (if (< i node-count)
                             (let [[b _] (mhg/add-node! b {:type :node :id i})]
                               (recur b (inc i)))
                             b))))]
      (is (= node-count (count (:nodes g))))
      (is (= node-count (count (mhg/nodes-of-type g :node)))))))

;;; ============================================================================
;;; Ported Goodies Tests
;;; ============================================================================

(deftest test-neighbors-seq
  (testing "neighbors-seq matches neighbors and respects filters"
    (let [g (-> (mhg/empty-graph)
                (mhg/add-node {:type :n})   ; 0
                (mhg/add-node {:type :n})   ; 1
                (mhg/add-node {:type :n})   ; 2
                (mhg/add-edge {:type :a :pins [{:node 0 :role :from}
                                               {:node 1 :role :to}]})
                (mhg/add-edge {:type :b :pins [{:node 0 :role :from}
                                               {:node 2 :role :to}]}))
          all (set (mhg/neighbors-seq g 0))
          only-a (vec (mhg/neighbors-seq g 0 :a nil))
          only-b (vec (mhg/neighbors-seq g 0 :b nil))]
      (is (= #{1 2} all))
      (is (= [1] only-a))
      (is (= [2] only-b)))))

(deftest test-build-ops
  (testing "build-ops applies node/edge/removal ops"
    (let [ops [[:node {:type :person :name "Alice"}]         ; nid 0
               [:node {:type :person :name "Bob"}]           ; nid 1
               [:edge {:type :knows
                       :pins [{:node 0 :role :knower}
                              {:node 1 :role :known}]}]
               [:rem-node 1]]
          g (mhg/build-ops (mhg/empty-graph) ops)]
      (is (contains? (:nodes g) 0))
      (is (not (contains? (:nodes g) 1)))
      (is (= #{} (mhg/incident-edges g 0))))))

(deftest test-reindex
  (testing "reindex rebuilds :ix from :nodes/:edges"
    (let [g0 (-> (mhg/empty-graph)
                 (mhg/add-node {:type :word :span {:start 10 :end 20}})
                 (mhg/add-node {:type :word :span {:start 30 :end 40}})
                 (mhg/add-edge {:type :next
                                :pins [{:node 0 :role :from}
                                       {:node 1 :role :to}]}))
          g-bad (-> g0
                    (assoc-in [:ix :incidence] {})         ; nuke incidence
                    (assoc-in [:ix :by-type] {})           ; nuke by-type
                    (assoc-in [:ix :spans] (sorted-map)))  ; nuke spans
          g (mhg/reindex g-bad)]
      (is (= #{0} (mhg/incident-edges g 0)))
      (is (= #{0} (mhg/edges-of-type g :next)))
      (is (contains? (get-in g [:ix :by-type :node]) :word))
      (is (contains? (get-in g [:ix :spans]) 10))
      (is (contains? (get-in g [:ix :spans]) 30)))))

(deftest test-interpret-ops
  (testing "interpret-ops executes operation nodes (homoiconic ops)"
    ;; Build an ops-graph that says: add Alice, add Bob, add :knows edge
    (let [ops-g (mhg/build (mhg/empty-graph)
                           (fn [b]
                             (let [[b _] (mhg/add-node! b {:type :operation
                                                           :op :add-node
                                                           :args {:type :person :name "Alice"}})
                                   [b _] (mhg/add-node! b {:type :operation
                                                           :op :add-node
                                                           :args {:type :person :name "Bob"}})
                                   [b _] (mhg/add-node! b {:type :operation
                                                           :op :add-edge
                                                           :args {:type :knows
                                                                  :pins [{:node 0 :role :knower}
                                                                         {:node 1 :role :known}]}})]
                               b)))]
      (let [g (mhg/interpret-ops ops-g)]
        (is (= #{"Alice" "Bob"} (set (map :name (vals (:nodes g))))))
        (is (= 1 (count (:edges g))))
        (is (= :knows (get-in g [:edges 0 :type])))))))

(deftest test-attach-element-and-children-ordered
  (testing "attach-element builds DOM-like subtree and children-ordered returns :i-sorted kids"
    (let [tree {:name "root"
                :children [{:name "a"}
                           {:name "b"}
                           {:name "c"}]}
          g (mhg/attach-element (mhg/empty-graph) tree nil 0)]
      ;; root should be nid 0; its children some 1..3, in :i order [0..2]
      (let [kids (mhg/children-ordered g 0)
            names (mapv #(get-in g [:nodes % :name]) kids)]
        (is (= 3 (count kids)))
        (is (= ["a" "b" "c"] names)))
      (is (= 3 (count (mhg/edges-of-type g :child)))))))

(deftest test-curried-dsl
  (testing "Curried node*/edge*/child* compose with begin*/finish*"
    (let [state (-> (mhg/begin* (mhg/empty-graph))
                    ((mhg/node* :root {:type :element :name "div"}))
                    ((mhg/node* :a    {:type :element :name "span"}))
                    ((mhg/node* :b    {:type :element :name "p"}))
                    ((mhg/child* :root :a 0))
                    ((mhg/child* :root :b 1)))
          g (mhg/finish* state)]
      (is (= 3 (count (:nodes g))))
      (is (= 2 (count (:edges g))))
      (is (= ["span" "p"]
             (mapv #(get-in g [:nodes % :name])
                   (mhg/children-ordered g 0)))))))

;;; ============================================================================
;;; Run Tests
;;; ============================================================================

(defn run-tests-cli []
  (run-tests 'aeonik.multi-hypergraph-test))
