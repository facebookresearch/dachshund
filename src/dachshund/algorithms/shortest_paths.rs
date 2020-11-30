/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{NodeBase, NodeEdgeBase};
use std::collections::{HashMap, HashSet, VecDeque};

type NodePredecessors = HashMap<NodeId, Vec<NodeId>>;
pub trait ShortestPaths: GraphBase {
    // Dikstra's algorithm for shortest paths. Returns distance and parent mappings
    fn get_shortest_paths(
        &self,
        source: NodeId,
        // nodes in the connected component to which source belongs. If you don't have
        // this available, just pass None. Returned distances will only be to those
        // nodes anyway (but this optimization saves some compute)
        nodes_in_connected_component: &Option<Vec<NodeId>>,
    ) -> (
        HashMap<NodeId, Option<usize>>,
        HashMap<NodeId, HashSet<NodeId>>,
    ) {
        // TODO: this should be changed to a binary heap
        let mut queue: HashSet<&NodeId> = HashSet::new();
        let mut dist: HashMap<NodeId, Option<usize>> = HashMap::new();
        let mut parents: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();

        let targets: Vec<NodeId> = match nodes_in_connected_component {
            Some(x) => x.iter().cloned().collect(),
            None => self.get_ids_iter().cloned().collect(),
        };
        for id in &targets {
            queue.insert(&id);
            dist.insert(*id, None);
            parents.insert(*id, HashSet::new());
        }
        *dist.get_mut(&source).unwrap() = Some(0);

        while !queue.is_empty() {
            let mut min_dist: Option<usize> = None;
            let mut u: Option<&NodeId> = None;
            // find next node u to visit
            for maybe_u in &queue {
                let d: Option<usize> = dist[maybe_u];
                if d != None && (min_dist == None || d.unwrap() < min_dist.unwrap()) {
                    min_dist = Some(d.unwrap());
                    u = Some(maybe_u);
                }
            }
            // remove u from queue
            queue.remove(u.unwrap());
            for e in self.get_node(*u.unwrap()).get_edges() {
                let v = &e.get_neighbor_id();
                if queue.contains(v) {
                    let alt = min_dist.unwrap() + 1;
                    if dist[v] == None || alt <= dist[v].unwrap() {
                        *dist.get_mut(v).unwrap() = Some(alt);
                        parents.get_mut(v).unwrap().insert(*u.unwrap());
                    }
                }
            }
        }
        parents.get_mut(&source).unwrap().insert(source);
        (dist, parents)
    }

    /// Single source paths in a unweighted, undirected graph by bfs.
    /// Returns nodes in the order of exploration, distances, and predecesors.
    fn get_shortest_paths_bfs(
        &self,
        source: NodeId,
    ) -> (
        Vec<NodeId>,          // nodes in nondecreasing order by distance
        HashMap<NodeId, u32>, // distances from source
        NodePredecessors,     // immediate predecessors
    ) {
        // Predecessors of v (nodes immediately before v on shortest path from source to v)
        let mut preds: NodePredecessors = HashMap::new();
        // Count of shortest paths to from source to v
        let mut shortest_path_counts: HashMap<NodeId, u32> = HashMap::new();
        // Distances from source to v
        let mut dists: HashMap<NodeId, i32> = HashMap::new();

        for node_id in self.get_ids_iter() {
            preds.insert(*node_id, Vec::new());
            shortest_path_counts.insert(*node_id, if node_id == &source { 1 } else { 0 });
            dists.insert(*node_id, if node_id == &source { 0 } else { -1 });
        }

        // A stack tracking the order in which we explored the nodes.
        let mut stack = Vec::new();
        // A queue tracking the remaining nodes to explore
        let mut queue = VecDeque::new();
        queue.push_back(source);

        while !queue.is_empty() {
            let v = queue.pop_front().unwrap();
            stack.push(v);
            let node = &self.get_node(v);
            for edge in node.get_edges() {
                let neighbor_id = edge.get_neighbor_id();
                // neighbor_id newly discovered
                if dists[&neighbor_id] < 0 {
                    queue.push_back(neighbor_id);
                    *dists.entry(neighbor_id).or_insert(0) = dists[&v] + 1;
                }
                // shortest path to neighbor_id via v?
                if dists[&neighbor_id] == dists[&v] + 1 {
                    *shortest_path_counts.entry(neighbor_id).or_insert(0) +=
                        shortest_path_counts[&v];
                    preds.get_mut(&neighbor_id).unwrap().push(v);
                }
            }
        }
        (stack, shortest_path_counts, preds)
    }

    fn retrace_parent_paths(
        &self,
        node_id: &NodeId,
        parent_ids: &HashSet<NodeId>,
        paths: &HashMap<NodeId, Vec<Vec<NodeId>>>,
    ) -> Vec<Vec<NodeId>> {
        let mut new_paths: Vec<Vec<NodeId>> = Vec::new();
        for parent_id in parent_ids {
            for parent_path in &paths[parent_id] {
                let mut new_path: Vec<NodeId> = parent_path.clone();
                new_path.push(*node_id);
                new_paths.push(new_path);
            }
        }
        new_paths
    }
    // enumerates shortest paths for a single source
    fn enumerate_shortest_paths(
        &self,
        dist: &HashMap<NodeId, Option<usize>>,
        parents: &HashMap<NodeId, HashSet<NodeId>>,
        destination: NodeId,
    ) -> HashMap<NodeId, Vec<Vec<NodeId>>> {
        let mut nodes_by_distance: HashMap<usize, Vec<NodeId>> = HashMap::new();
        for (node_id, distance) in dist {
            if *node_id != destination {
                let d = distance.unwrap();
                nodes_by_distance.entry(d).or_insert_with(Vec::new);
                nodes_by_distance.get_mut(&d).unwrap().push(*node_id);
            }
        }
        nodes_by_distance.insert(0 as usize, vec![destination]);

        let mut distances: Vec<usize> = nodes_by_distance.keys().cloned().collect::<Vec<usize>>();
        distances.sort();

        // all the paths from a source to the destination;
        let mut paths: HashMap<NodeId, Vec<Vec<NodeId>>> = HashMap::new();
        paths.insert(destination, vec![vec![]]);
        for d in distances {
            let nodes: &Vec<NodeId> = nodes_by_distance.get(&d).unwrap();
            for node_id in nodes {
                let parent_ids = parents.get(node_id).unwrap();
                let new_paths = self.retrace_parent_paths(node_id, &parent_ids, &paths);
                paths.insert(*node_id, new_paths);
            }
        }
        paths
    }
}
