/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate nalgebra as na;
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_builder::GraphBuilder;
use crate::dachshund::id_types::{EdgeTypeId, NodeId};
use crate::dachshund::node::{Node, NodeEdge};
use crate::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use std::collections::{BTreeMap, HashMap, HashSet};

use rand::prelude::*;

pub struct SimpleUndirectedGraphBuilder {}
impl GraphBuilder<SimpleUndirectedGraph> for SimpleUndirectedGraphBuilder {
    fn _new(
        nodes: HashMap<NodeId, Node>,
        core_ids: Vec<NodeId>,
        non_core_ids: Vec<NodeId>,
    ) -> CLQResult<SimpleUndirectedGraph> {
        assert!(core_ids.len() == non_core_ids.len());

        Ok(SimpleUndirectedGraph {
            nodes,
            ids: core_ids,
        })
    }
}

impl SimpleUndirectedGraphBuilder {
    // builds a graph from a vector of IDs. Repeated edges are ignored.
    // Edges only need to be provided once (this being an undirected graph)
    #[allow(clippy::ptr_arg)]
    pub fn from_vector(data: &Vec<(i64, i64)>) -> SimpleUndirectedGraph {
        let mut ids: BTreeMap<NodeId, HashSet<NodeId>> = BTreeMap::new();
        for (id1, id2) in data {
            ids.entry(NodeId::from(*id1))
                .or_insert_with(HashSet::new)
                .insert(NodeId::from(*id2));
            ids.entry(NodeId::from(*id2))
                .or_insert_with(HashSet::new)
                .insert(NodeId::from(*id1));
        }
        let edge_type_id = EdgeTypeId::from(0 as usize);
        let mut nodes: HashMap<NodeId, Node> = HashMap::new();
        for (id, neighbors) in ids.into_iter() {
            nodes.insert(
                id,
                Node {
                    node_id: id,
                    edges: neighbors
                        .iter()
                        .map(|x| NodeEdge::new(edge_type_id, *x))
                        .collect(),
                    neighbors: neighbors
                        .iter()
                        .map(|x| (*x, vec![NodeEdge::new(edge_type_id, *x)]))
                        .collect(),
                    // meaningless
                    is_core: true,
                    non_core_type: None,
                },
            );
        }
        SimpleUndirectedGraph {
            ids: nodes.keys().cloned().collect(),
            nodes,
        }
    }

    // Build a graph with n vertices with every possible edge.
    pub fn get_complete_graph(n: u64) -> SimpleUndirectedGraph {
        let mut v = Vec::new();
        for i in 1..n {
            for j in i + 1..=n {
                v.push((i, j));
            }
        }
        SimpleUndirectedGraphBuilder::from_vector(
            &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
        )
    }

    // Build a graph with a sequence of n vertices with an edge between
    // each pair of successive vertices.
    pub fn get_path_graph(n: u64) -> SimpleUndirectedGraph {
        let mut v = Vec::new();
        for i in 0..n {
            v.push((i, (i + 1)));
        }

        SimpleUndirectedGraphBuilder::from_vector(
            &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
        )
    }

    // Build a graph with a sequence of n vertices with an edge between
    // each pair of successive vertices, plus an edge between the first and
    // last vertices.
    pub fn get_cycle_graph(n: u64) -> SimpleUndirectedGraph {
        let mut v = Vec::new();
        for i in 0..n {
            v.push((i, (i + 1) % n));
        }

        SimpleUndirectedGraphBuilder::from_vector(
            &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
        )
    }

    // Builds an Erdos-Renyi graph on n edges with p vertices.
    // (Each possible edge is added to the graph independently at random with
    //  probability p.)
    // [TODO] Switch to the faster implementation using geometric distributions
    // for sparse graphs.
    pub fn get_er_graph(n: u64, p: f64) -> SimpleUndirectedGraph {
        let mut v = Vec::new();
        let mut rng = rand::thread_rng();

        for i in 1..n {
            for j in i + 1..=n {
                if rng.gen::<f64>() < p {
                    v.push((i, j));
                }
            }
        }

        SimpleUndirectedGraphBuilder::from_vector(
            &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
        )
    }
}
