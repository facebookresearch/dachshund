/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_builder_base::GraphBuilderBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::SimpleNode;
use crate::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use std::collections::{BTreeMap, BTreeSet};
extern crate fxhash;
use fxhash::FxHashMap;
use itertools::Itertools;

use rand::prelude::*;
pub struct SimpleUndirectedGraphBuilder {}

pub trait TSimpleUndirectedGraphBuilder: GraphBuilderBase<RowType=(i64, i64)> {

    // Build a graph with n vertices with every possible edge.
    fn get_complete_graph(&self, n: u64) -> Self::GraphType {
        let mut v = Vec::new();
        for i in 1..n {
            for j in i + 1..=n {
                v.push((i, j));
            }
        }
        self.from_vector(&v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect())
    }

    // Build a graph with a sequence of n vertices with an edge between
    // each pair of successive vertices.
    fn get_path_graph(&self, n: u64) -> Self::GraphType {
        let mut v = Vec::new();
        for i in 0..n {
            v.push((i, (i + 1)));
        }

        self.from_vector(&v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect())
    }

    // Build a graph with a sequence of n vertices with an edge between
    // each pair of successive vertices, plus an edge between the first and
    // last vertices.
    fn get_cycle_graph(&self, n: u64) -> Self::GraphType {
        let mut v = Vec::new();
        for i in 0..n {
            v.push((i, (i + 1) % n));
        }

        self.from_vector(&v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect())
    }

    // Builds an Erdos-Renyi graph on n edges with p vertices.
    // (Each possible edge is added to the graph independently at random with
    //  probability p.)
    // [TODO] Switch to the faster implementation using geometric distributions
    // for sparse graphs.
    fn get_er_graph(&self, n: u64, p: f64) -> Self::GraphType {
        let mut v = Vec::new();
        let mut rng = rand::thread_rng();

        for i in 1..n {
            for j in i + 1..=n {
                if rng.gen::<f64>() < p {
                    v.push((i, j));
                }
            }
        }

        self.from_vector(&v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect())
    }

    fn get_node_ids(data: &Vec<(i64, i64)>) -> BTreeMap<NodeId, BTreeSet<NodeId>> {
        let mut ids: BTreeMap<NodeId, BTreeSet<NodeId>> = BTreeMap::new();
        for (id1, id2) in data {
            ids.entry(NodeId::from(*id1))
                .or_insert_with(BTreeSet::new)
                .insert(NodeId::from(*id2));
            ids.entry(NodeId::from(*id2))
                .or_insert_with(BTreeSet::new)
                .insert(NodeId::from(*id1));
        }
        ids
    }
    fn get_nodes(ids: BTreeMap<NodeId, BTreeSet<NodeId>>) -> FxHashMap<NodeId, SimpleNode> {
        let mut nodes: FxHashMap<NodeId, SimpleNode> = FxHashMap::default();
        for (id, neighbors) in ids.into_iter() {
            nodes.insert(
                id,
                SimpleNode {
                    node_id: id,
                    neighbors: neighbors,
                },
            );
        }
        nodes
    }
}

impl GraphBuilderBase for SimpleUndirectedGraphBuilder {
    type GraphType = SimpleUndirectedGraph;
    type RowType = (i64, i64);

    // builds a graph from a vector of IDs. Repeated edges are ignored.
    // Edges only need to be provided once (this being an undirected graph)
    fn from_vector(&self, data: &Vec<(i64, i64)>) -> SimpleUndirectedGraph {
        let ids = Self::get_node_ids(data);
        let nodes = Self::get_nodes(ids);
        SimpleUndirectedGraph {
            ids: nodes.keys().cloned().collect(),
            nodes,
        }
    }
}
impl TSimpleUndirectedGraphBuilder for SimpleUndirectedGraphBuilder {}

pub struct SimpleUndirectedGraphBuilderWithCliques {
    cliques: Vec<BTreeSet<NodeId>>,
}

impl SimpleUndirectedGraphBuilderWithCliques {
    pub fn new(cliques: Vec<BTreeSet<NodeId>>) -> Self {
        Self { cliques }
    }
}
impl TSimpleUndirectedGraphBuilder for SimpleUndirectedGraphBuilderWithCliques {}
impl GraphBuilderBase for SimpleUndirectedGraphBuilderWithCliques {
    type GraphType = SimpleUndirectedGraph;
    type RowType = (i64, i64);

    // builds a graph from a vector of IDs. Repeated edges are ignored.
    // Edges only need to be provided once (this being an undirected graph)
    fn from_vector(&self, data: &Vec<(i64, i64)>) -> SimpleUndirectedGraph {
        let ids = Self::get_node_ids(data);
        let mut nodes = Self::get_nodes(ids);
        for clique in &self.cliques {
            for comb in clique.iter().combinations(2) {
                let id1 = comb.get(0).unwrap().clone();
                let id2 = comb.get(1).unwrap().clone();
                let node = nodes.get_mut(id1).unwrap();
                node.neighbors.insert(*id2);
                let node = nodes.get_mut(id2).unwrap();
                node.neighbors.insert(*id1);
            }
        }
        SimpleUndirectedGraph {
            ids: nodes.keys().cloned().collect(),
            nodes,
        }
    }
}
