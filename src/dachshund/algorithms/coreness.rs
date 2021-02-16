/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;

use crate::dachshund::algorithms::connected_components::ConnectedComponents;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use core::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter::FromIterator;

use fxhash::FxHashSet;

type OrderedNodeSet = BTreeSet<NodeId>;
type OrderedEdgeSet = BTreeSet<(NodeId, NodeId)>;

pub trait Coreness: GraphBase + ConnectedComponents {
    fn _get_k_cores(&self, k: usize, removed: &mut FxHashSet<NodeId>) -> Vec<Vec<NodeId>> {
        // [BUG] This algorithm has a bug. See simple_graph.rs tests.
        let mut queue: OrderedNodeSet = self.get_ids_iter().cloned().collect();
        let mut num_neighbors: HashMap<NodeId, usize> = self
            .get_nodes_iter()
            .map(|x| (x.get_id(), x.degree()))
            .collect();
        // iteratively delete all nodes w/ degree less than k
        while !queue.is_empty() {
            let id = queue.pop_first().unwrap();
            // this assumes no multiple connections to neighbors
            if num_neighbors[&id] < k {
                removed.insert(id);
                for e in self.get_node(id).get_edges() {
                    let nid = e.get_neighbor_id();
                    if !removed.contains(&nid) {
                        queue.insert(nid);
                        *num_neighbors.get_mut(&id).unwrap() -= 1;
                        *num_neighbors.get_mut(&nid).unwrap() -= 1;
                    }
                }
            }
        }
        self._get_connected_components(Some(removed), None)
    }

    fn get_k_cores(&self, k: usize) -> Vec<Vec<NodeId>> {
        let mut removed: FxHashSet<NodeId> = FxHashSet::default();
        self._get_k_cores(k, &mut removed)
    }

    fn _init_bin_starts(
        &self,
        ordered_nodes: &Vec<NodeId>,
        degree: &HashMap<NodeId, usize>,
    ) -> Vec<usize> {
        // bin_boundaries[i] tracks the leftmost index in ordered_nodes
        // such that degree of the node at that index >= i
        let mut bin_boundaries = vec![0];
        let mut current_degree = 0;
        for i in 0..ordered_nodes.len() {
            let new_degree = degree[&ordered_nodes[i]];
            if new_degree > current_degree {
                // create one new bin for each possible degree value
                for _ in current_degree + 1..=new_degree {
                    bin_boundaries.push(i);
                }
                current_degree = new_degree;
            }
        }
        bin_boundaries
    }

    fn get_coreness(&self) -> (Vec<Vec<Vec<NodeId>>>, HashMap<NodeId, usize>) {
        let coreness = self.get_coreness_values();
        let core_assignments = self._get_core_assignments(&coreness);
        (core_assignments, coreness)
    }

    fn _get_core_assignments(&self, coreness: &HashMap<NodeId, usize>) -> Vec<Vec<Vec<NodeId>>> {
        // Use coreness mapping to compute connected components of each k-core.
        let mut nodes: Vec<NodeId> = coreness.keys().cloned().collect();
        nodes.sort_unstable_by_key(|node_id| coreness[node_id]);
        // coreness_bin_starts[i] = j means i core consists of nodes[j..]
        // so when computing connected components, we exclude
        // initial segments of nodes up to the starts of these bins.
        let coreness_bin_starts = self._init_bin_starts(&nodes, &coreness);

        let mut core_assignments: Vec<Vec<Vec<NodeId>>> = Vec::new();
        let mut removed: FxHashSet<NodeId>;
        for bin_start in &coreness_bin_starts[1..] {
            removed = nodes[..*bin_start].iter().cloned().collect();
            core_assignments.push(self._get_connected_components(Some(&removed), None));
        }
        core_assignments
    }

    fn get_coreness_values(&self) -> HashMap<NodeId, usize> {
        // Traverse the nodes in increasing order of degree to calculate coreness.
        // See: https://arxiv.org/abs/cs/0310049 for an explanation of the bookkeeping details.

        // The initial value for the coreness of each node is its degree.
        let mut coreness: HashMap<NodeId, usize> = self
            .get_nodes_iter()
            .map(|x| (x.get_id(), x.degree()))
            .collect();

        // Nodes in increasing order of coreness. We process this in order
        // and keep in order as we delete edges.
        let mut nodes: Vec<NodeId> = coreness.keys().cloned().collect();
        nodes.sort_unstable_by_key(|node_id| coreness[node_id]);

        let mut bin_starts = self._init_bin_starts(&nodes, &coreness);

        let mut node_idx: HashMap<NodeId, usize> = HashMap::new();
        for (i, &node) in nodes.iter().enumerate() {
            node_idx.insert(node, i);
        }

        let mut neighbors: HashMap<NodeId, FxHashSet<NodeId>> = HashMap::new();
        for node in self.get_nodes_iter() {
            neighbors.insert(
                node.get_id(),
                FxHashSet::<NodeId>::from_iter(node.get_edges().map(|edge| edge.get_neighbor_id())),
            );
        }

        for i in 0..nodes.len() {
            let node_id = nodes[i];
            let node_nbrs: Vec<NodeId> = neighbors.get(&node_id).unwrap().iter().cloned().collect();
            for nbr_id in node_nbrs {
                let nbr_coreness = coreness[&nbr_id];
                if nbr_coreness > coreness[&node_id] {
                    neighbors.get_mut(&nbr_id).unwrap().remove(&node_id);
                    let nbr_idx = node_idx[&nbr_id];
                    let nbr_bin_start = bin_starts[nbr_coreness];

                    let nbr_idx_ptr = node_idx.get_mut(&nbr_id).unwrap() as *mut usize;
                    let bin_start_node_idx_ptr =
                        node_idx.get_mut(&nodes[nbr_bin_start]).unwrap() as *mut usize;
                    unsafe {
                        std::ptr::swap(nbr_idx_ptr, bin_start_node_idx_ptr);
                    }
                    nodes.swap(nbr_idx, nbr_bin_start);

                    bin_starts[nbr_coreness] += 1;
                    *coreness.entry(nbr_id).or_default() -= 1;
                }
            }
        }

        coreness
    }

    fn get_coreness_anomaly(&self, coreness: &HashMap<NodeId, usize>) -> HashMap<NodeId, f64> {
        // Calculate the coreness anomaly score of all nodes as the absolute
        // value of the difference between the logs of the ranks by
        // degree and coreness.

        // See algorithm Core-A in https://www.cs.cmu.edu/~kijungs/papers/kcoreICDM2016.pdf
        let mut anomaly_scores = HashMap::new();
        let core_ranks = averaged_ties_ranking(&coreness);
        let deg_ranks = averaged_ties_ranking(
            &self
                .get_nodes_iter()
                .map(|x| (x.get_id(), x.degree()))
                .collect(),
        );
        for node in self.get_ordered_node_ids() {
            anomaly_scores.insert(node, (core_ranks[&node].ln() - deg_ranks[&node].ln()).abs());
        }
        anomaly_scores
    }

    fn _get_k_trusses(
        &self,
        k: usize,
        ignore_nodes: &FxHashSet<NodeId>,
    ) -> (Vec<OrderedEdgeSet>, HashSet<OrderedNodeSet>) {
        let mut neighbors: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
        let mut edges: OrderedEdgeSet = BTreeSet::new();
        for node in self.get_nodes_iter() {
            // [TODO] This step is unncessary now.
            neighbors.insert(
                node.get_id(),
                HashSet::from_iter(
                    node.get_edges()
                        .map(|x| x.get_neighbor_id())
                        .filter(|x| !ignore_nodes.contains(x)),
                ),
            );
            for e in node.get_edges() {
                let id_pair: (NodeId, NodeId);
                let node_id = node.get_id();
                let neighbor_id = e.get_neighbor_id();
                if node_id < neighbor_id {
                    id_pair = (node_id, neighbor_id);
                } else {
                    id_pair = (neighbor_id, node_id);
                }
                edges.insert(id_pair);
            }
        }
        let mut changes = true;
        let mut ignore_edges: HashSet<(NodeId, NodeId)> = HashSet::new();
        while changes {
            changes = false;
            let mut to_remove: Vec<(NodeId, NodeId)> = Vec::new();
            for (id1, id2) in &edges {
                let n1 = &neighbors[&id1];
                let n2 = &neighbors[&id2];
                let intersection = n1.intersection(n2);
                if intersection.count() < k - 2 {
                    to_remove.push((*id1, *id2));
                    neighbors.get_mut(id1).unwrap().remove(id2);
                    neighbors.get_mut(id2).unwrap().remove(id1);
                }
            }
            for e in &to_remove {
                changes = true;
                edges.remove(&e);
                ignore_edges.insert(*e);
            }
        }
        let (components, num_components) =
            self._get_connected_components_membership(None, Some(&ignore_edges));
        let mut trusses: Vec<OrderedEdgeSet> = vec![BTreeSet::new(); num_components];
        for (id, idx) in &components {
            // reusing the neighbors sets from above
            for nid in &neighbors[&id] {
                // will only return (lesser_id, greater_id) for an UndirectedGraph
                if components[nid] == *idx && id < nid {
                    let eid = (*id, *nid);
                    if !ignore_edges.contains(&eid) && edges.contains(&eid) {
                        trusses[*idx].insert(eid);
                    }
                }
            }
        }
        let filtered_trusses: Vec<OrderedEdgeSet> =
            trusses.into_iter().filter(|x| !x.is_empty()).collect();
        let truss_nodes = filtered_trusses
            .iter()
            .map(|y| BTreeSet::from_iter(y.iter().map(|x| x.0).chain(y.iter().map(|x| x.1))))
            .collect::<HashSet<OrderedNodeSet>>();
        (filtered_trusses, truss_nodes)
    }
    fn get_k_trusses(&self, k: usize) -> (Vec<OrderedEdgeSet>, HashSet<OrderedNodeSet>) {
        // Basic algorithm: https://louridas.github.io/rwa/assignments/finding-trusses/

        // ignore_nodes will contain all the irrelevant nodes after
        // calling self._get_k_cores();
        let mut ignore_nodes: FxHashSet<NodeId> = FxHashSet::default();
        // this really only works for an undirected graph
        self._get_k_cores(k - 1, &mut ignore_nodes);
        self._get_k_trusses(k, &ignore_nodes)
    }
}

pub fn averaged_ties_ranking(scores: &HashMap<NodeId, usize>) -> HashMap<NodeId, f64> {
    // Given a map from NodeIds to values, create a new map from those NodeIds to their rank.
    // In the case of ties, all tied keys get the same, averaged rank.
    // e.g. {1: 10, 2: 20, 3: 15, 4: 20, 5: 25} -> {5: 1, 4: 2.5, 2: 2.5, 3: 4, 1: 5}

    let mut ranking = HashMap::new();
    let mut sorted_nodes: Vec<(&NodeId, &usize)> = scores.iter().collect();
    sorted_nodes.sort_unstable_by_key(|(_node, value)| Reverse(*value));

    let mut tied_nodes: Vec<&NodeId> = Vec::<&NodeId>::new();
    let mut tied_rank: f64;
    let mut last_value: Option<usize> = None;

    for (i, (node, &value)) in sorted_nodes.into_iter().enumerate() {
        if last_value == None || Some(value) == last_value {
            tied_nodes.push(node);
        } else {
            tied_rank = (i as f64) - (tied_nodes.len() - 1) as f64 / 2.0;
            for tied_node in tied_nodes {
                ranking.insert(*tied_node, tied_rank as f64);
            }
            tied_nodes = vec![node];
        }
        last_value = Some(value);
    }
    tied_rank = (scores.len() as f64) - (tied_nodes.len() - 1) as f64 / 2.0;
    for tied_node in tied_nodes {
        ranking.insert(*tied_node, tied_rank as f64);
    }
    ranking
}
