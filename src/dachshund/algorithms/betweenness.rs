/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::algorithms::connectivity::Connectivity;
use crate::dachshund::algorithms::shortest_paths::ShortestPaths;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use std::collections::HashMap;

pub trait Betweenness: GraphBase + Connectivity + ShortestPaths {
    fn get_node_betweenness_starting_from_sources(
        &self,
        sources: &[NodeId],
        check_is_connected: bool,
        nodes_in_connected_component: Option<Vec<NodeId>>,
    ) -> Result<HashMap<NodeId, f64>, &'static str> {
        if self.count_nodes() == 0 {
            return Err("Graph is empty");
        }
        if check_is_connected && !self.get_is_connected().unwrap() {
            return Err("Graph should be connected to compute betweenness.");
        }
        let mut path_counts: HashMap<NodeId, f64> = HashMap::new();
        for node_id in self.get_ids_iter() {
            path_counts.insert(*node_id, 0.0);
        }

        for source in sources.iter() {
            let (dist, parents) = self.get_shortest_paths(*source, &nodes_in_connected_component);
            let shortest_paths = self.enumerate_shortest_paths(&dist, &parents, *source);
            for paths in shortest_paths.values() {
                let weight: f64 = 0.5 / paths.len() as f64;
                for path in paths {
                    for id in path.iter().skip(1).rev().skip(1) {
                        *path_counts.get_mut(id).unwrap() += weight;
                    }
                }
            }
        }
        Ok(path_counts)
    }
    // graph must be connected if you're calling this
    fn get_node_betweenness(&self) -> Result<HashMap<NodeId, f64>, &'static str> {
        let ids: Vec<NodeId> = self.get_ids_iter().cloned().collect();
        self.get_node_betweenness_starting_from_sources(&ids, true, None)
    }

    fn get_node_betweenness_brandes(&self) -> Result<HashMap<NodeId, f64>, &'static str> {
        // Algorithm: Brandes, Ulrik. A Faster Algorithm For Betweeness Centrality.
        // https://www.eecs.wsu.edu/~assefaw/CptS580-06/papers/brandes01centrality.pdf

        if self.count_nodes() == 0 {
            return Err("Graph is empty");
        }
        if !self.get_is_connected().unwrap() {
            return Err("Graph should be connected to compute betweenness.");
        }

        let mut betweenness: HashMap<NodeId, f64> = HashMap::new();
        for node_id in self.get_ids_iter() {
            betweenness.insert(*node_id, 0.0);
        }

        for source in self.get_ids_iter() {
            let (mut stack, shortest_path_counts, preds) = self.get_shortest_paths_bfs(*source);

            let mut dependencies: HashMap<NodeId, f64> = HashMap::new();
            for node_id in self.get_ids_iter() {
                dependencies.insert(*node_id, 0.0);
            }

            // Process nodes in order of nonincreasing distance from source to leverage
            // recurrence relation in accumulating pair dependencies.
            while !stack.is_empty() {
                let w = stack.pop().unwrap();
                for pred in &preds[&w] {
                    *dependencies.entry(*pred).or_insert(0.0) += (0.5 + dependencies[&w])
                        * (shortest_path_counts[&pred] as f64 / shortest_path_counts[&w] as f64)
                }
                if w != *source {
                    *betweenness.entry(w).or_insert(0.0) += dependencies[&w]
                }
            }
        }

        Ok(betweenness)
    }
}
