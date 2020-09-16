/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate nalgebra as na;
use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::{GraphId, NodeId, NodeTypeId};
use crate::dachshund::node::{Node, NodeEdge};
use crate::dachshund::row::EdgeRow;
use std::collections::{HashMap, HashSet};

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
                id,             // node_id,
                true,           // is_core,
                None,           // non_core_type,
                Vec::new(),     // edges,
                HashMap::new(), //neighbors
            );
            node_map.insert(id, node);
        }
        for &id in non_core_ids {
            let node = Node::new(
                id,                           // node_id,
                false,                        // is_core,
                Some(non_core_type_ids[&id]), // non_core_type,
                Vec::new(),                   // edges,
                HashMap::new(),               // neighbors
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

            let source_node = node_map
                .get_mut(&r.source_id)
                .ok_or_else(CLQError::err_none)?;

            if !source_node.neighbors.contains_key(&r.target_id) {
                source_node.neighbors.insert(r.target_id, Vec::new());
            }
            source_node
                .neighbors
                .get_mut(&r.target_id)
                .unwrap()
                .push(NodeEdge::new(r.edge_type_id, r.target_id));

            // probably unnecessary.
            node_map
                .get_mut(&r.source_id)
                .ok_or_else(CLQError::err_none)?
                .edges
                .push(NodeEdge::new(r.edge_type_id, r.target_id));

            // edges with the same source and target type should not be repeated
            if r.source_type_id != r.target_type_id {
                let target_node = node_map
                    .get_mut(&r.target_id)
                    .ok_or_else(CLQError::err_none)?;

                if !target_node.neighbors.contains_key(&r.source_id) {
                    target_node.neighbors.insert(r.source_id, Vec::new());
                }
                target_node
                    .neighbors
                    .get_mut(&r.source_id)
                    .unwrap()
                    .push(NodeEdge::new(r.edge_type_id, r.source_id));

                target_node
                    .edges
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
            let node_degree: usize = node.degree();
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
                for n in node.edges.iter() {
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
