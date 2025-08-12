# `aeonik/hypergraph`

> **A homoiconic hypergraph library for Clojure** — minimal, composable, and built for structural sharing.

Very early work in progress.

## ✨ Features

- **Hyperedges** that can connect *any number* of nodes
- **Typed nodes & edges** with fast indices for queries
- **Span indexing** for spatial/text/document range queries
- **Immutable core** with batch updates via transients
- **Graphs of graphs** — treat a graph as a node, compose layers
- **Homoiconic operations** — graphs can describe their own construction

## 🧠 Philosophy

> *Everything is a graph.*

- Even graphs themselves are nodes in meta-graphs  
- A minimal core with infinite extensibility through composition  
- Overlapping spans? → another graph layer  
- Custom indices? → another graph  
- Build operations? → also a graph  

---

## 🚀 Quick Start

```clojure
(ns demo
  (:require [aeonik.hypergraph :as hg]))

;; Create an empty graph
(def g0 (hg/empty-graph))

;; Add nodes
(def g1 (-> g0
            (hg/add-node {:type :person :name "Alice"})
            (hg/add-node {:type :person :name "Bob"})))

;; Add a hyperedge with roles
(def g2 (hg/add-edge g1 {:type :knows
                         :pins [{:node 0 :role :knower}
                                {:node 1 :role :known}]}))

;; Query neighbors
(hg/neighbors g2 0)
;; => [1]

```

## ⚡ Batch Building

For large builds, use transients to avoid intermediate allocations:

```clojure
(def g3
  (hg/build g0
    (fn [b]
      (-> b
          (hg/add-node! {:type :doc :span {:start 0 :end 100}})
          (hg/add-node! {:type :doc :span {:start 50 :end 150}})))))

```
## 🧩 Graph Composition

Graphs are just nodes in bigger graphs:

```clojure
(def base
  (-> (hg/empty-graph)
      (hg/add-node {:type :text :content "Hello world"
                    :span {:start 0 :end 11}})))

(def annotations
  (-> (hg/empty-graph)
      (hg/add-node {:type :highlight :color :yellow
                    :span {:start 0 :end 5}})
      (hg/add-node {:type :highlight :color :blue
                    :span {:start 6 :end 11}})))

(def composite
  (hg/compose-layers {:base base
                      :annotations annotations}))
```
## 🔍 Queries & Traversals

```clojure
;; Span coverage
(hg/covering base 4)
;; => {:kind :node, :start 0, :end 11, :ids #{0}}

;; Breadth-first search
(hg/bfs-seq g2 [0])
;; => (0 1)

;; Depth-first search
(hg/dfs-seq g2 [0])
;; => (0 1)
```

## 📦 Installation

Add to deps.edn:

```clojure 
aeonik/hypergraph {:git/url "https://github.com/aeonik/hypergraph"
                   :git/sha "<latest-sha>"}
```
## 🧪 Development

Run tests:
 
``` sh
clojure -T:build test
```
(there are no tests yet)

📜 License

EPL © 2025 David Connett
