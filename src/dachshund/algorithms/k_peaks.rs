/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;

use crate::dachshund::algorithms::coreness::Coreness;
use crate::dachshund::graph_base::{GraphBase};
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use crate::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use std::collections::{BTreeSet, HashMap, HashSet};
use crate::{GraphBuilderBase};


pub trait KPeaks: GraphBase + Coreness {

    fn get_new_coreness_values(&self, h_nodes: &HashSet<NodeId>) -> HashMap<NodeId, usize> {
        let mut edges : Vec<(i64, i64)> = Vec::new();

        for node in self.get_nodes_iter() {
            for e in node.get_edges() {
                if h_nodes.contains(&node.get_id()) && h_nodes.contains(&e.get_neighbor_id()) {
                    edges.push((node.get_id().value(), e.get_neighbor_id().value()));
                }
            }
        }

        let mut builder = SimpleUndirectedGraphBuilder {};
        let graph = &builder.from_vector(edges).unwrap();

        h_nodes.into_iter().fold(graph.get_coreness_values(), |mut acc, n_id| {
            acc.entry(*n_id).or_insert(0);
            acc
        })
    }

    fn get_k_peak_mountain_assignment(&self) -> (HashMap<NodeId, i32>, HashMap<usize, HashMap<NodeId, usize>>) {
        let mut mountain_assignments: HashMap<NodeId, Vec<usize>> = self.get_nodes_iter()
            .map(|x| (x.get_id(), vec![0,0]))
            .collect();

        let mut h_nodes: HashSet<NodeId> = mountain_assignments.keys().cloned().collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let mut curr_core_values = self.get_coreness_values();
        let mut current_plotmountain_id = 0;
        let mut peak_numbers: HashMap<NodeId, i32> = HashMap::new();

        while !h_nodes.is_empty() {
            let k_value = curr_core_values.values().cloned().max().unwrap();
            let curr = curr_core_values.clone();
            let degeneracy_nodes: Vec<_> =  curr.iter()
                .filter_map(|(key, &val)| if val == k_value { Some(key) } else { None })
                .collect();

            for d_id in degeneracy_nodes {
                h_nodes.remove(&d_id);
                peak_numbers.entry(*d_id).or_insert(*curr_core_values.get(d_id).unwrap() as i32);
                if let Some(x) = mountain_assignments.get_mut(d_id) {
                    if *curr_core_values.get(d_id).unwrap() > *x.first().unwrap() {
                        *x = vec![*curr_core_values.get(d_id).unwrap(), current_plotmountain_id]
                    }
                }
            }

            let new_core_values = self.get_new_coreness_values(&h_nodes);
            for (n_id, coreness) in &new_core_values {
                if let Some(x) = mountain_assignments.get_mut(n_id){
                    if curr_core_values.get(n_id) != None && *curr_core_values.get(n_id).unwrap() - coreness > *x.first().unwrap() {
                        *x = vec![*curr_core_values.get(&n_id).unwrap() - coreness, current_plotmountain_id]
                    }
                }
            }

            current_plotmountain_id += 1;
            curr_core_values = new_core_values.clone();
        }

        let mut mountain_id_core: HashMap<usize, HashMap<NodeId, usize>> = HashMap::new();
        for (n_id, coreness) in self.get_coreness_values() {
            if let Some(x) = mountain_assignments.get_mut(&n_id) {
                mountain_id_core.entry(*x.get(1).unwrap()).or_insert(HashMap::new());
                for (m_id, m_nodes) in mountain_id_core.iter_mut() {
                    if m_id == x.get(1).unwrap() {
                        m_nodes.entry(n_id).or_insert(coreness);
                    }
                }
            }
        }

        (peak_numbers, mountain_id_core)
    }
}
