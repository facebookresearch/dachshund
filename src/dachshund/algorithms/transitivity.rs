/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use std::collections::HashSet;

pub trait Transitivity: GraphBase {
    // Triangles : Number of triangles a node participates in.
    fn triangle_count(&self, node_id: NodeId) -> usize {
        let node = self.get_node(node_id);
        let mut neighbor_ids: HashSet<NodeId> = HashSet::new();
        for ne in node.get_edges() {
            neighbor_ids.insert(ne.get_neighbor_id());
        }

        let mut triangle_count = 0;
        for ne in node.get_edges() {
            let neighbor = self.get_node(ne.get_neighbor_id());
            triangle_count += neighbor.count_ties_with_ids(&neighbor_ids);
        }

        triangle_count / 2
    }

    // Triples : pairs of neighbors of a given node.
    fn triples_count(&self, node_id: NodeId) -> usize {
        let num_neighbors = &self.get_node(node_id).degree();
        num_neighbors * (num_neighbors - 1) / 2
    }

    // Transitivity: 3 * number of triangles  / number of triples
    fn get_transitivity(&self) -> f64 {
        let num_triangles =
            Iterator::sum::<usize>(self.get_ids_iter().map(|x| self.triangle_count(*x)));

        let num_triples =
            Iterator::sum::<usize>(self.get_ids_iter().map(|x| self.triples_count(*x)));

        num_triangles as f64 / num_triples as f64
    }

    // Approximate Transitivity
    // k~=26,000 gives an approximation w/ <1% chance of an error of more than 1 percentage point.
    // See http://jgaa.info/accepted/2005/SchankWagner2005.9.2.pdf for approximation guarantees.
    fn get_approx_transitivity(&self, samples: usize) -> f64 {
        let ordered_nodes = self
            .get_nodes_iter()
            .filter(|node| node.degree() >= 2)
            .collect::<Vec<_>>();

        let triples_counts: Vec<usize> = self
            .get_nodes_iter()
            .filter(|node| node.degree() >= 2)
            .map(|node| self.triples_count(node.get_id()))
            .collect();
        let dist = WeightedIndex::new(triples_counts).unwrap();

        let mut successes = 0;
        let mut rng = rand::thread_rng();
        for _i in 0..samples {
            // Choose a random node weighted by degree.
            let v = &ordered_nodes[dist.sample(&mut rng)];

            // Choose 2 random nodes that are neighbors of j
            let mut random_neighbors = v.get_edges().choose_multiple(&mut rng, 2).into_iter();
            let next_random_neighbor = random_neighbors.next();
            let u_id = next_random_neighbor.unwrap().get_neighbor_id();
            let w_id = random_neighbors.next().unwrap().get_neighbor_id();

            // TODO: No constant time way to check if there's an edge?
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
