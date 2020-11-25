/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate nalgebra as na;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use rand::Rng;
use rand::prelude::*;
use std::collections::HashSet;

pub trait Clustering: GraphBase {
    fn get_clustering_coefficient(&self, id: NodeId) -> Option<f64> {
        let node = self.get_node(id);
        let mut neighbor_ids: HashSet<NodeId> = HashSet::new();
        for ne in node.get_edges() {
            neighbor_ids.insert(ne.get_neighbor_id());
        }
        let num_neighbors: usize = neighbor_ids.len();
        if num_neighbors <= 1 {
            return None;
        }
        let mut num_ties: usize = 0;
        for ne in node.get_edges() {
            let neighbor = &self.get_node(ne.get_neighbor_id());
            num_ties += neighbor.count_ties_with_ids(&neighbor_ids);
        }
        // different from degree -- this is the number of distinct neighbors,
        // not the number of edges -- a neighbor may be connected by multiple
        // edges.
        Some(num_ties as f64 / ((num_neighbors * (num_neighbors - 1)) as f64))
    }
    fn get_avg_clustering(&self) -> f64 {
        let coefs = self
            .get_ids_iter()
            .map(|x| self.get_clustering_coefficient(*x))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<f64>>();
        Iterator::sum::<f64>(coefs.iter()) / coefs.len() as f64
    }
    // Approximate Clustering - Randomly sample neighbors of nodes w/ degree at least 2.
    // k~=26,000 gives an approximation w/ <1% chance of an error of more than 1 percentage point.
    // See http://jgaa.info/accepted/2005/SchankWagner2005.9.2.pdf for approximation guarantees.
    fn get_approx_avg_clustering(&self, samples: usize) -> f64 {
        let ordered_nodes = self
            .get_nodes_iter()
            .filter(|node| node.degree() >= 2)
            .map(|node| node)
            .collect::<Vec<_>>();

        let n = ordered_nodes.len();
        let mut successes = 0;
        let mut rng = rand::thread_rng();

        for _i in 0..samples {
            // Pick a random node with degree at least 2.
            let v = &ordered_nodes[rng.gen_range(0, n)];

            // Choose 2 random nodes that are neighbors of j
            let mut random_neighbors = v.get_edges().choose_multiple(&mut rng, 2).into_iter();
            let next_random_neighbor = random_neighbors.next();
            let u_id = next_random_neighbor.unwrap().get_neighbor_id();
            let w_id = random_neighbors.next().unwrap().get_neighbor_id();

            // If they're connected, increment l.
            // TODO: No O(1) way to check if there's an edge?
            for edge in self.get_node(u_id).get_edges() {
                if edge.get_neighbor_id() == w_id {
                    successes += 1;
                    break;
                }
            }
        }
        (successes as f64) / (samples as f64)
    }
}
