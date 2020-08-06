/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate nalgebra as na;
use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::{EdgeTypeId, GraphId, NodeId, NodeTypeId};
use crate::dachshund::node::{Node, NodeEdge};
use crate::dachshund::row::EdgeRow;
use crate::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Keeps track of a bipartite graph composed of "core" and "non-core" nodes. Only core ->
/// non-core connections may exist in the graph. The neighbors of core nodes are non-cores, the
/// neighbors of non-core nodes are cores. Graph edges are stored in the neighbors field of
/// each node. If the id of a node is known, its Node object can be retrieved via the
/// nodes HashMap. To iterate over core and non-core nodes, the struct also provides the
/// core_ids and non_core_ids vectors.
pub struct Graph {
    pub nodes: HashMap<NodeId, Node>,
    pub core_ids: Vec<NodeId>,
    pub non_core_ids: Vec<NodeId>,
}
impl GraphBase for Graph {
    fn get_core_ids(&self) -> &Vec<NodeId> {
        &self.core_ids
    }
    fn get_non_core_ids(&self) -> Option<&Vec<NodeId>> {
        Some(&self.non_core_ids)
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
        num_edges
    }
}
/// Trait encapsulting the logic required to build a graph from a set of edge
/// rows. Currently used to build typed graphs.
pub trait GraphBuilder<TGraph: GraphBase>
where
    Self: Sized,
    TGraph: Sized,
{
    fn _new(
        nodes: HashMap<NodeId, Node>,
        core_ids: Vec<NodeId>,
        non_core_ids: Vec<NodeId>,
    ) -> CLQResult<TGraph>;
    // initializes nodes in the graph with empty neighbors fields.
    fn init_nodes(
        core_ids: &[NodeId],
        non_core_ids: &[NodeId],
        non_core_type_ids: &HashMap<NodeId, NodeTypeId>,
    ) -> HashMap<NodeId, Node> {
        let mut node_map: HashMap<NodeId, Node> = HashMap::new();
        for &id in core_ids {
            let node = Node::new(
                id,         // node_id,
                true,       // is_core,
                None,       // non_core_type,
                Vec::new(), // neighbors,
            );
            node_map.insert(id, node);
        }
        for &id in non_core_ids {
            let node = Node::new(
                id,                           // node_id,
                false,                        // is_core,
                Some(non_core_type_ids[&id]), // non_core_type,
                Vec::new(),                   // neighbors,
            );
            node_map.insert(id, node);
        }
        node_map
    }

    /// given a set of initialized Nodes, populates the respective neighbors fields
    /// appropriately.
    fn populate_edges(rows: &[EdgeRow], node_map: &mut HashMap<NodeId, Node>) -> CLQResult<()> {
        for r in rows.iter() {
            assert!(node_map.contains_key(&r.source_id));
            assert!(node_map.contains_key(&r.target_id));
            node_map
                .get_mut(&r.source_id)
                .ok_or_else(CLQError::err_none)?
                .neighbors
                .push(NodeEdge::new(r.edge_type_id, r.target_id));
            // edges with the same source and target type should not be repeated
            if r.source_type_id != r.target_type_id {
                node_map
                    .get_mut(&r.target_id)
                    .ok_or_else(CLQError::err_none)?
                    .neighbors
                    .push(NodeEdge::new(r.edge_type_id, r.source_id));
            }
        }
        Ok(())
    }
    /// Trims edges greedily, until all edges in the graph have degree at least min_degree.
    /// Note that this function does not delete any nodes -- just finds nodes to delete. It is
    /// called by `prune`, which actually does the deletion.
    fn trim_edges(node_map: &mut HashMap<NodeId, Node>, min_degree: &usize) -> HashSet<NodeId> {
        let mut degree_map: HashMap<NodeId, usize> = HashMap::new();
        for (node_id, node) in node_map.iter() {
            let node_degree: usize = node.neighbors.len();
            degree_map.insert(*node_id, node_degree);
        }
        let mut nodes_to_delete: HashSet<NodeId> = HashSet::new();
        loop {
            let mut nodes_to_update: HashSet<NodeId> = HashSet::new();
            for (node_id, node_degree) in degree_map.iter() {
                if node_degree < min_degree && !nodes_to_delete.contains(node_id) {
                    nodes_to_update.insert(*node_id);
                    nodes_to_delete.insert(*node_id);
                }
            }
            if nodes_to_update.is_empty() {
                break;
            }
            for node_id in nodes_to_update.iter() {
                let node: &Node = &node_map[node_id];
                for n in node.neighbors.iter() {
                    let neighbor_node_id: NodeId = n.target_id;
                    let current_degree: usize = degree_map[&neighbor_node_id];
                    degree_map.insert(neighbor_node_id, current_degree - 1);
                }
            }
        }
        nodes_to_delete
    }
    /// creates a TGraph object from a vector of rows. Client must provide
    /// graph_id which must match with each row's graph_id. If min_degree
    /// is provided, the graph is additionally pruned.
    fn new(graph_id: GraphId, rows: &[EdgeRow], min_degree: Option<usize>) -> CLQResult<TGraph> {
        let mut source_ids: HashSet<NodeId> = HashSet::new();
        let mut target_ids: HashSet<NodeId> = HashSet::new();
        let mut target_type_ids: HashMap<NodeId, NodeTypeId> = HashMap::new();
        for r in rows.iter() {
            assert!(graph_id == r.graph_id);
            source_ids.insert(r.source_id);
            target_ids.insert(r.target_id);
            target_type_ids.insert(r.target_id, r.target_type_id);
        }

        // warrant a canonical order on the id vectors
        let mut source_ids_vec: Vec<NodeId> = source_ids.into_iter().collect();
        source_ids_vec.sort();
        let mut target_ids_vec: Vec<NodeId> = target_ids.into_iter().collect();
        target_ids_vec.sort();

        let mut node_map: HashMap<NodeId, Node> =
            Self::init_nodes(&source_ids_vec, &target_ids_vec, &target_type_ids);
        Self::populate_edges(rows, &mut node_map)?;
        let mut graph = Self::_new(node_map, source_ids_vec, target_ids_vec)?;
        if let Some(min_degree) = min_degree {
            graph = Self::prune(graph, rows, min_degree)?;
        }
        Ok(graph)
    }
    /// Takes an already-built graph and the edge rows used to create it, returning a
    /// new graph, where all nodes are assured to have degree at least min_degree.
    /// The provision of a TGraph is necessary, since the notion of "degree" does
    /// not make sense outside of a graph.
    fn prune(graph: TGraph, rows: &[EdgeRow], min_degree: usize) -> CLQResult<TGraph> {
        let mut target_type_ids: HashMap<NodeId, NodeTypeId> = HashMap::new();
        for r in rows.iter() {
            target_type_ids.insert(r.target_id, r.target_type_id);
        }
        let (filtered_source_ids, filtered_target_ids, filtered_rows) =
            Self::get_filtered_sources_targets_rows(graph, min_degree, rows);
        let mut filtered_node_map: HashMap<NodeId, Node> =
            Self::init_nodes(&filtered_source_ids, &filtered_target_ids, &target_type_ids);
        Self::populate_edges(&filtered_rows, &mut filtered_node_map)?;
        Self::_new(filtered_node_map, filtered_source_ids, filtered_target_ids)
    }
    /// called by `prune`, finds source and target nodes to exclude, as well as edges to exclude
    /// when rebuilding the graph from a filtered vector of `EdgeRows`.
    fn get_filtered_sources_targets_rows(
        mut graph: TGraph,
        min_degree: usize,
        rows: &[EdgeRow],
    ) -> (Vec<NodeId>, Vec<NodeId>, Vec<EdgeRow>) {
        let exclude_nodes: HashSet<NodeId> = Self::trim_edges(graph.get_mut_nodes(), &min_degree);
        let filtered_source_ids: Vec<NodeId> = graph
            .get_core_ids()
            .iter()
            .filter(|x| !exclude_nodes.contains(x))
            .cloned()
            .collect();
        let filtered_target_ids: Vec<NodeId> = graph
            .get_non_core_ids()
            .unwrap()
            .iter()
            .filter(|x| !exclude_nodes.contains(x))
            .cloned()
            .collect();
        let filtered_rows: Vec<EdgeRow> = rows
            .iter()
            .filter(|x| {
                !(exclude_nodes.contains(&x.source_id) || (exclude_nodes.contains(&x.target_id)))
            })
            .cloned()
            .collect();
        // todo: make member of struct
        (filtered_source_ids, filtered_target_ids, filtered_rows)
    }
}

pub struct TypedGraphBuilder {}
impl GraphBuilder<Graph> for TypedGraphBuilder {
    fn _new(
        nodes: HashMap<NodeId, Node>,
        core_ids: Vec<NodeId>,
        non_core_ids: Vec<NodeId>,
    ) -> CLQResult<Graph> {
        Ok(Graph {
            nodes,
            core_ids,
            non_core_ids,
        })
    }
}

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
                    neighbors: neighbors
                        .into_iter()
                        .map(|x| NodeEdge::new(edge_type_id, x))
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
}
