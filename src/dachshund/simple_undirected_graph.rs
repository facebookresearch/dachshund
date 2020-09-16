/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate nalgebra as na;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::Node;
use na::{DMatrix, DVector};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::iter::FromIterator;

type GraphMatrix = DMatrix<f64>;
type OrderedNodeSet = BTreeSet<NodeId>;
type OrderedEdgeSet = BTreeSet<(NodeId, NodeId)>;
type NodePredecessors = HashMap<NodeId, Vec<NodeId>>;

/// Keeps track of a simple undirected graph, composed of nodes without any type information.
pub struct SimpleUndirectedGraph {
    pub nodes: HashMap<NodeId, Node>,
    pub ids: Vec<NodeId>,
}
impl GraphBase for SimpleUndirectedGraph {
    /// core and non-core IDs are the same for a `SimpleUndirectedGraph`.
    fn get_core_ids(&self) -> &Vec<NodeId> {
        &self.ids
    }
    /// core and non-core IDs are the same for a `SimpleUndirectedGraph`.
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>> {
        Some(&self.ids)
    }
    fn get_mut_nodes(&mut self) -> &mut HashMap<NodeId, Node> {
        &mut self.nodes
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.nodes.contains_key(&node_id)
    }
    fn get_node(&self, node_id: NodeId) -> &Node {
        &self.nodes[&node_id]
    }
    fn count_edges(&self) -> usize {
        let mut num_edges: usize = 0;
        for node in self.nodes.values() {
            num_edges += node.neighbors.len();
        }
        num_edges / 2
    }
}
impl SimpleUndirectedGraph {
    pub fn as_input_rows(&self, graph_id: usize) -> String {
        let mut rows: Vec<String> = Vec::new();
        for (id, node) in &self.nodes {
            for e in &node.edges {
                if *id < e.target_id {
                    rows.push(format!(
                        "{}\t{}\t{}",
                        graph_id,
                        id.value(),
                        e.target_id.value()
                    ));
                }
            }
        }
        rows.join("\n")
    }
    pub fn get_node_degree(&self, id: NodeId) -> usize {
        self.nodes[&id].degree()
    }
    pub fn get_clustering_coefficient(&self, id: NodeId) -> Option<f64> {
        let node: &Node = &self.nodes[&id];
        let mut neighbor_ids: HashSet<NodeId> = HashSet::new();
        for ne in &node.edges {
            neighbor_ids.insert(ne.target_id);
        }
        let num_neighbors: usize = neighbor_ids.len();
        if num_neighbors <= 1 {
            return None;
        }
        let mut num_ties: usize = 0;
        for ne in &node.edges {
            let neighbor: &Node = &self.nodes[&ne.target_id];
            num_ties += neighbor.count_ties_with_ids(&neighbor_ids);
        }
        // different from degree -- this is the number of distinct neighbors,
        // not the number of edges -- a neighbor may be connected by multiple
        // edges.
        Some(num_ties as f64 / ((num_neighbors * (num_neighbors - 1)) as f64))
    }
    pub fn get_avg_clustering(&self) -> f64 {
        let coefs = self
            .ids
            .iter()
            .map(|x| self.get_clustering_coefficient(*x))
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect::<Vec<f64>>();
        Iterator::sum::<f64>(coefs.iter()) / coefs.len() as f64
    }

    // Triangles : Number of triangles a node participates in.
    pub fn triangle_count(&self, node_id: NodeId) -> usize {
        let node: &Node = &self.nodes[&node_id];
        let mut neighbor_ids: HashSet<NodeId> = HashSet::new();
        for ne in &node.edges {
            neighbor_ids.insert(ne.target_id.clone());
        }

        let mut triangle_count = 0;
        for ne in &node.edges {
            let neighbor: &Node = &self.nodes[&ne.target_id];
            triangle_count += neighbor.count_ties_with_ids(&neighbor_ids);
        }

        triangle_count / 2
    }

    // Triples : pairs of neighbors of a given node.
    pub fn triples_count(&self, node_id: NodeId) -> usize {
        let num_neighbors = &self.nodes[&node_id].edges.len();
        num_neighbors * (num_neighbors - 1) / 2
    }

    // Transitivity: 3 * number of triangles  / number of triples
    pub fn get_transitivity(&self) -> f64 {
        let num_triangles =
            Iterator::sum::<usize>(self.ids.iter().map(|x| self.triangle_count(*x)));

        let num_triples = Iterator::sum::<usize>(self.ids.iter().map(|x| self.triples_count(*x)));

        num_triangles as f64 / num_triples as f64
    }

    // Approximate Clustering - Randomly sample neighbors of nodes w/ degree at least 2.
    // k~=26,000 gives an approximation w/ <1% chance of an error of more than 1 percentage point.
    // See http://jgaa.info/accepted/2005/SchankWagner2005.9.2.pdf for approximation guarantees.
    pub fn get_approx_avg_clustering(&self, samples: usize) -> f64 {
        let ordered_nodes = self
            .nodes
            .iter()
            .filter(|(_node_id, node)| node.degree() >= 2)
            .map(|(_node_id, node)| node)
            .collect::<Vec<&Node>>();

        let n = ordered_nodes.len();
        let mut successes = 0;
        let mut rng = rand::thread_rng();

        for _i in 0..samples {
            // Pick a random node with degree at least 2.
            let v = &ordered_nodes[rng.gen_range(0, n)];

            // Choose 2 random nodes that are neighbors of j
            let mut random_neighbors = v.edges.choose_multiple(&mut rng, 2).map(|x| x.target_id);
            let u_id = random_neighbors.next().unwrap();
            let w_id = random_neighbors.next().unwrap();

            // If they're connected, increment l.
            // TODO: No O(1) way to check if there's an edge?
            for edge in &self.nodes[&u_id].edges {
                if edge.target_id == w_id {
                    successes += 1;
                    break;
                }
            }
        }
        (successes as f64) / (samples as f64)
    }

    // Approximate Transitivity
    // k~=26,000 gives an approximation w/ <1% chance of an error of more than 1 percentage point.
    // See http://jgaa.info/accepted/2005/SchankWagner2005.9.2.pdf for approximation guarantees.
    pub fn get_approx_transitivity(&self, samples: usize) -> f64 {
        let ordered_nodes = self
            .nodes
            .iter()
            .filter(|(_node_id, node)| node.degree() >= 2)
            .map(|(_node_id, node)| node)
            .collect::<Vec<&Node>>();

        let triples_counts: Vec<usize> = self
            .nodes
            .iter()
            .filter(|(_node_id, node)| node.degree() >= 2)
            .map(|(node_id, _node)| self.triples_count(*node_id))
            .collect();
        let dist = WeightedIndex::new(triples_counts).unwrap();

        let mut successes = 0;
        let mut rng = rand::thread_rng();
        for _i in 0..samples {
            // Choose a random node weighted by degree.
            let v = &ordered_nodes[dist.sample(&mut rng)];

            // Choose 2 random nodes that are neighbors of j
            let mut random_neighbors = v.edges.choose_multiple(&mut rng, 2).map(|x| x.target_id);
            let u_id = random_neighbors.next().unwrap();
            let w_id = random_neighbors.next().unwrap();

            // TODO: No constant time way to check if there's an edge?
            for edge in &self.nodes[&u_id].edges {
                if edge.target_id == w_id {
                    successes += 1;
                    break;
                }
            }
        }

        (successes as f64) / (samples as f64)
    }

    // Dikstra's algorithm for shortest paths. Returns distance and parent mappings
    pub fn get_shortest_paths(
        &self,
        source: NodeId,
        // nodes in the connected component to which source belongs. If you don't have
        // this available, just pass None. Returned distances will only be to those
        // nodes anyway (but this optimization saves some compute)
        nodes_in_connected_component: Option<&Vec<NodeId>>,
    ) -> (
        HashMap<NodeId, Option<usize>>,
        HashMap<NodeId, HashSet<NodeId>>,
    ) {
        // TODO: this should be changed to a binary heap
        let mut queue: HashSet<&NodeId> = HashSet::new();
        let mut dist: HashMap<NodeId, Option<usize>> = HashMap::new();
        let mut parents: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();

        let targets: &Vec<NodeId> = match nodes_in_connected_component {
            Some(x) => x,
            None => &self.ids,
        };
        for id in targets {
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
            for e in &self.nodes[u.unwrap()].edges {
                let v = &e.target_id;
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
    pub fn get_shortest_paths_bfs(
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

        for node_id in self.nodes.keys() {
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
            let node = &self.nodes[&v];
            for edge in &node.edges {
                let neighbor_id = edge.target_id;
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
    pub fn enumerate_shortest_paths(
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

    pub fn visit_nodes_from_root(&self, root: &NodeId, visited: &mut OrderedNodeSet) {
        let mut to_visit: Vec<NodeId> = Vec::new();
        to_visit.push(*root);
        while !to_visit.is_empty() {
            let node_id = to_visit.pop().unwrap();
            let node = &self.nodes[&node_id];
            for edge in &node.edges {
                let neighbor_id = edge.target_id;
                if !visited.contains(&neighbor_id) {
                    to_visit.push(neighbor_id);
                }
            }
            visited.insert(node_id);
        }
    }
    pub fn get_is_connected(&self) -> Result<bool, &'static str> {
        let mut visited: OrderedNodeSet = BTreeSet::new();
        if self.nodes.is_empty() {
            return Err("Graph is empty");
        }
        let root = self.nodes.keys().next().unwrap();
        self.visit_nodes_from_root(&root, &mut visited);
        Ok(visited.len() == self.nodes.len())
    }
    pub fn create_empty() -> Self {
        Self {
            nodes: HashMap::new(),
            ids: Vec::new(),
        }
    }
    pub fn get_node_betweenness_starting_from_sources(
        &self,
        sources: &[NodeId],
        check_is_connected: bool,
        nodes_in_connected_component: Option<&Vec<NodeId>>,
    ) -> Result<HashMap<NodeId, f64>, &'static str> {
        if self.nodes.is_empty() {
            return Err("Graph is empty");
        }
        if check_is_connected && !self.get_is_connected().unwrap() {
            return Err("Graph should be connected to compute betweenness.");
        }
        let mut path_counts: HashMap<NodeId, f64> = HashMap::new();
        for node_id in self.nodes.keys() {
            path_counts.insert(*node_id, 0.0);
        }

        for source in sources.iter() {
            let (dist, parents) = self.get_shortest_paths(*source, nodes_in_connected_component);
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
    pub fn get_node_betweenness(&self) -> Result<HashMap<NodeId, f64>, &'static str> {
        self.get_node_betweenness_starting_from_sources(&self.ids, true, None)
    }

    fn get_ordered_node_ids(&self) -> Vec<NodeId> {
        let mut node_ids: Vec<NodeId> = self.nodes.keys().cloned().collect();
        node_ids.sort();
        node_ids
    }

    pub fn get_node_betweenness_brandes(&self) -> Result<HashMap<NodeId, f64>, &'static str> {
        // Algorithm: Brandes, Ulrik. A Faster Algorithm For Betweeness Centrality.
        // https://www.eecs.wsu.edu/~assefaw/CptS580-06/papers/brandes01centrality.pdf

        if self.nodes.is_empty() {
            return Err("Graph is empty");
        }
        if !self.get_is_connected().unwrap() {
            return Err("Graph should be connected to compute betweenness.");
        }

        let mut betweenness: HashMap<NodeId, f64> = HashMap::new();
        for node_id in self.nodes.keys() {
            betweenness.insert(*node_id, 0.0);
        }

        for source in self.nodes.keys() {
            let (mut stack, shortest_path_counts, preds) = self.get_shortest_paths_bfs(*source);

            let mut dependencies: HashMap<NodeId, f64> = HashMap::new();
            for node_id in self.nodes.keys() {
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

    pub fn get_degree_matrix(&self) -> (GraphMatrix, Vec<NodeId>) {
        let node_ids = self.get_ordered_node_ids();
        let diag: Vec<f64> = node_ids
            .iter()
            .map(|x| self.nodes[x].degree() as f64)
            .collect();
        (
            GraphMatrix::from_diagonal(&DVector::from_row_slice(&diag)),
            node_ids,
        )
    }

    pub fn get_adjacency_matrix_given_node_ids(&self, node_ids: &[NodeId]) -> GraphMatrix {
        let num_nodes = node_ids.len();
        let mut data: Vec<f64> = vec![0.0; num_nodes * num_nodes];
        let pos_map: HashMap<NodeId, usize> = node_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, item)| (item, i))
            .collect();

        for (i, node_id) in node_ids.iter().enumerate() {
            for e in &self.nodes[node_id].edges {
                let j = pos_map.get(&e.target_id).unwrap();
                let pos = i * num_nodes + j;
                data[pos] += 1.0;
            }
        }
        GraphMatrix::from_vec(num_nodes, num_nodes, data)
    }
    pub fn get_adjacency_matrix(&self) -> (GraphMatrix, Vec<NodeId>) {
        let node_ids = self.get_ordered_node_ids();
        (
            self.get_adjacency_matrix_given_node_ids(&node_ids),
            node_ids,
        )
    }

    pub fn get_laplacian_matrix(&self) -> (GraphMatrix, Vec<NodeId>) {
        let (deg_mat, node_ids) = self.get_degree_matrix();
        let adj_mat = self.get_adjacency_matrix_given_node_ids(&node_ids);
        (deg_mat - adj_mat, node_ids)
    }
    // Algebraic Connectivity, or the Fiedler Measure, is the second-smallest eigenvalue of the graph Laplacian.
    // The lower the value, the less decomposable the graph's adjacency matrix is. Thanks to the nalgebra
    // crate computing this is quite straightforward.
    pub fn get_algebraic_connectivity(&self) -> f64 {
        let (laplacian, _ids) = self.get_laplacian_matrix();
        let eigen = laplacian.symmetric_eigen();
        let mut eigenvalues: Vec<f64> = eigen.eigenvalues.iter().cloned().collect();
        eigenvalues.sort_by(|a, b| a.partial_cmp(b).unwrap());
        eigenvalues[1]
    }

    pub fn get_eigenvector_centrality(&self, eps: f64, max_iter: usize) -> HashMap<NodeId, f64> {
        let (adj_mat, node_ids) = self.get_adjacency_matrix();
        // Power iteration adaptation from
        // https://www.sci.unich.it/~francesc/teaching/network/eigenvector.html

        let n = node_ids.len();
        let mut x0: GraphMatrix = GraphMatrix::zeros(1, n);
        let mut x1: GraphMatrix = GraphMatrix::repeat(1, n, 1.0 / n as f64);
        let mut iter: usize = 0;
        while (&x0 - &x1).abs().sum() > eps && iter < max_iter {
            x0 = x1;
            x1 = &x0 * &adj_mat;
            let m = x1.max();
            x1 /= m;
            iter += 1;
        }
        let mut ev: HashMap<NodeId, f64> = HashMap::new();
        for i in 0..n {
            ev.insert(node_ids[i], x1[i]);
        }
        ev
    }
    // returns a hashmap of the form node_id => component_id -- can be turned
    // in to a vector of node_ids inside _get_connected_components.
    pub fn _get_connected_components_membership(
        &self,
        ignore_nodes: Option<&HashSet<NodeId>>,
        ignore_edges: Option<&HashSet<(NodeId, NodeId)>>,
    ) -> (HashMap<NodeId, usize>, usize) {
        let mut components: HashMap<NodeId, usize> = HashMap::new();
        let mut queue: OrderedNodeSet = BTreeSet::new();
        for id in self.nodes.keys() {
            if ignore_nodes.is_none() || !ignore_nodes.unwrap().contains(id) {
                queue.insert(*id);
            }
        }
        let mut idx = 0;
        while !queue.is_empty() {
            let id = queue.pop_first().unwrap();
            let distinct_nodes: Vec<NodeId> = self.nodes[&id]
                .edges
                .iter()
                .map(|x| x.target_id)
                .filter(|x| {
                    ignore_edges.is_none()
                        || (!ignore_edges.unwrap().contains(&(id, *x))
                            && !ignore_edges.unwrap().contains(&(*x, id)))
                })
                .collect();
            let mut q2: OrderedNodeSet = BTreeSet::from_iter(distinct_nodes.into_iter());

            while !q2.is_empty() {
                let nid = q2.pop_first().unwrap();
                if ignore_nodes.is_none() || !ignore_nodes.unwrap().contains(&nid) {
                    components.insert(nid, idx);
                    if queue.contains(&nid) {
                        queue.remove(&nid);
                    }
                    for e in &self.nodes[&nid].edges {
                        let nid2 = e.target_id;
                        if (ignore_nodes.is_none() || !ignore_nodes.unwrap().contains(&nid2))
                            && (ignore_edges.is_none()
                                || (!ignore_edges.unwrap().contains(&(nid, nid2))
                                    && !ignore_edges.unwrap().contains(&(nid2, nid))))
                            && !components.contains_key(&nid2)
                        {
                            q2.insert(nid2);
                        }
                    }
                }
            }
            idx += 1;
        }
        (components, idx)
    }
    pub fn _get_connected_components(
        &self,
        ignore_nodes: Option<&HashSet<NodeId>>,
        ignore_edges: Option<&HashSet<(NodeId, NodeId)>>,
    ) -> Vec<Vec<NodeId>> {
        let (components, n) = self._get_connected_components_membership(ignore_nodes, ignore_edges);
        let mut v: Vec<Vec<NodeId>> = vec![Vec::new(); n];
        for (nid, core_idx) in components {
            v[core_idx].push(nid);
        }
        v
    }
    pub fn get_connected_components(&self) -> Vec<Vec<NodeId>> {
        self._get_connected_components(None, None)
    }

    pub fn _get_k_cores(&self, k: usize, removed: &mut HashSet<NodeId>) -> Vec<Vec<NodeId>> {
        let mut queue: OrderedNodeSet = self.nodes.keys().cloned().collect();
        let mut num_neighbors: HashMap<NodeId, usize> = self
            .nodes
            .values()
            .map(|x| {
                (
                    x.node_id,
                    HashSet::<NodeId>::from_iter(x.edges.iter().map(|y| y.target_id)).len(),
                )
            })
            .collect();
        // iteratively delete all nodes w/ degree less than k
        while !queue.is_empty() {
            let id = queue.pop_first().unwrap();
            // this assumes no multiple connections to neighbors
            if num_neighbors[&id] < k {
                removed.insert(id);
                for e in &self.nodes[&id].edges {
                    let nid = e.target_id;
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

    pub fn get_k_cores(&self, k: usize) -> Vec<Vec<NodeId>> {
        let mut removed: HashSet<NodeId> = HashSet::new();
        self._get_k_cores(k, &mut removed)
    }

    pub fn get_coreness(&self) -> (Vec<Vec<Vec<NodeId>>>, HashMap<NodeId, usize>) {
        let mut core_assignments: Vec<Vec<Vec<NodeId>>> = Vec::new();
        let mut removed: HashSet<NodeId> = HashSet::new();
        let mut k: usize = 0;
        while removed.len() < self.nodes.len() {
            k += 1;
            core_assignments.push(self._get_k_cores(k, &mut removed))
        }
        let mut coreness: HashMap<NodeId, usize> = HashMap::new();
        for i in (0..k).rev() {
            for ids in &core_assignments[i] {
                for id in ids {
                    if !coreness.contains_key(id) {
                        coreness.insert(*id, i + 1);
                    }
                }
            }
        }
        (core_assignments, coreness)
    }

    pub fn _get_k_trusses(
        &self,
        k: usize,
        ignore_nodes: &HashSet<NodeId>,
    ) -> (Vec<OrderedEdgeSet>, HashSet<OrderedNodeSet>) {
        let mut neighbors: HashMap<NodeId, HashSet<NodeId>> = HashMap::new();
        let mut edges: OrderedEdgeSet = BTreeSet::new();
        for node in self.nodes.values() {
            // [TODO] This step is unncessary now.
            neighbors.insert(
                node.node_id,
                HashSet::from_iter(
                    node.edges
                        .iter()
                        .map(|x| x.target_id)
                        .filter(|x| !ignore_nodes.contains(x)),
                ),
            );
            for e in &node.edges {
                let id_pair: (NodeId, NodeId);
                if node.node_id < e.target_id {
                    id_pair = (node.node_id, e.target_id);
                } else {
                    id_pair = (e.target_id, node.node_id);
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
    pub fn get_k_trusses(&self, k: usize) -> (Vec<OrderedEdgeSet>, HashSet<OrderedNodeSet>) {
        // Basic algorithm: https://louridas.github.io/rwa/assignments/finding-trusses/

        // ignore_nodes will contain all the irrelevant nodes after
        // calling self._get_k_cores();
        let mut ignore_nodes: HashSet<NodeId> = HashSet::new();
        // this really only works for an undirected graph
        self._get_k_cores(k - 1, &mut ignore_nodes);
        self._get_k_trusses(k, &ignore_nodes)
    }
}
