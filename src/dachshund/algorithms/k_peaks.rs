/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;

use crate::dachshund::algorithms::coreness::Coreness;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use crate::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use crate::GraphBuilderBase;
use std::collections::{HashMap, HashSet};

use fxhash::FxHashSet;

pub trait KPeaks: GraphBase + Coreness
where
    Self::NodeType: NodeBase<NodeIdType = NodeId, NodeSetType = FxHashSet<NodeId>>,
    <Self::NodeType as NodeBase>::NodeEdgeType: NodeEdgeBase<NodeIdType = NodeId>,
{
    // Function that computes new coreness values from the set of nodes provided
    fn get_new_coreness_values(&self, nodes: &HashSet<NodeId>) -> HashMap<NodeId, usize> {
        let mut edges: Vec<(i64, i64)> = Vec::new();

        // Determine if the current edge is fully contained in the list of nodes and push it if it is
        for node in self.get_nodes_iter() {
            for e in node.get_edges() {
                if nodes.contains(&node.get_id()) && nodes.contains(&e.get_neighbor_id()) {
                    edges.push((node.get_id().value(), e.get_neighbor_id().value()));
                }
            }
        }

        // Create new graph from set of edges selected
        let mut builder = SimpleUndirectedGraphBuilder {};
        let graph = &builder.from_vector(edges).unwrap();

        // If a node is missing from the list of coreness values (not contained in an edge)
        // add a coreness value of 0 to hashmap for that node
        nodes
            .iter()
            .fold(graph.get_coreness_values(), |mut acc, n_id| {
                acc.entry(*n_id).or_insert(0);
                acc
            })
    }

    // Function to compute peak numbers and keep track of which k-contour (removal affected each node the most)
    fn get_k_peak_mountain_assignment(
        &self,
    ) -> (HashMap<NodeId, i32>, HashMap<usize, HashMap<NodeId, usize>>) {
        // Hashmap with nodeID -> (largest drop, mountain assignment)
        let mut mountain_assignments: HashMap<NodeId, [usize; 2]> = self
            .get_nodes_iter()
            .map(|x| (x.get_id(), [0; 2]))
            .collect();

        // Remaining graph nodes that we have not yet processed
        let mut remaining_nodes: HashSet<NodeId> = mountain_assignments
            .keys()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let mut curr_core_values = self.get_coreness_values(); // current core numbers of the graph
        let orig_core_values = curr_core_values.clone(); // original core numbers of the graph
        let mut current_mountain_index = 0; // 'current_mountain_index' keeps track of numbering of the plot-mountains
        let mut peak_numbers: HashMap<NodeId, i32> = HashMap::new(); // Hashmap of nodeID -> peak_number

        while !remaining_nodes.is_empty() {
            // While there are still nodes left in the list, repeat the decomposition
            let k_value = curr_core_values.values().cloned().max().unwrap(); // The k-value of the degeneracy core of G
            let curr = curr_core_values.clone();
            let degeneracy_nodes: Vec<_> = curr
                .iter() // Nodes in the k-contour whose peak number will be their core number
                .filter_map(|(key, &val)| if val == k_value { Some(key) } else { None })
                .collect();

            for d_id in degeneracy_nodes {
                // For nodes in the k_contour the removal causes its core number to drop to 0,
                // We check to see if this drop is greater than the drop in core number observed for these
                // nodes in previous iterations
                remaining_nodes.remove(d_id); // Remove the k-contour node and insert peak numbers
                let curr_core_value = *curr_core_values.get(d_id).unwrap();
                peak_numbers.entry(*d_id).or_insert(curr_core_value as i32);
                if let Some(x) = mountain_assignments.get_mut(d_id) {
                    if curr_core_value > *x.first().unwrap() {
                        *x = [curr_core_value, current_mountain_index]
                    }
                }
            }

            let new_core_values = self.get_new_coreness_values(&remaining_nodes); // Compute new coreness values
            for (n_id, coreness) in &new_core_values {
                if let Some(x) = mountain_assignments.get_mut(n_id) {
                    let current_drop = *curr_core_values.get(n_id).unwrap() - coreness; // Check to see if we should update the drop in core number
                    if current_drop > *x.first().unwrap() {
                        *x = [current_drop, current_mountain_index]
                    }
                }
            }

            current_mountain_index += 1; // Update id of the mountain
            curr_core_values = new_core_values.clone(); // Update core numbers from last iteration
        }

        // Return a Hashmap of hashmap with mountain assignments
        // Key is mountain number
        // Value is a hashmap of nodes assigned to that mountain
        // Key of inner dict are nodes and value is peak number
        let mut mountain_id_core: HashMap<usize, HashMap<NodeId, usize>> = HashMap::new();
        for (n_id, coreness) in orig_core_values {
            if let Some(x) = mountain_assignments.get_mut(&n_id) {
                mountain_id_core
                    .entry(*x.get(1).unwrap())
                    .or_insert(HashMap::new());
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
