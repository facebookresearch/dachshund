/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;

use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::graph_builder_base::{GraphBuilderBase, GraphBuilderBaseWithPreProcessing};
use crate::dachshund::id_types::{EdgeTypeId, GraphId, NodeLabel, NodeTypeId};
use crate::dachshund::node::{Node, NodeBase, NodeEdge};
use crate::dachshund::row::EdgeRow;
use crate::dachshund::typed_graph::{LabeledGraph, TypedGraph};
use fxhash::FxHashMap;
use std::collections::{BTreeSet, HashMap, HashSet};

/// In the TypedGraph world, we use the type NodeLabel as an alias for the NodeId
/// type. Internally we represent node ids with u32s of 0...n.
pub struct TypedGraphBuilder {
    pub min_degree: Option<usize>,
    pub graph_id: GraphId,
}
impl GraphBuilderBase for TypedGraphBuilder {
    type GraphType = TypedGraph;
    type RowType = EdgeRow;

    fn from_vector(&mut self, rows: Vec<EdgeRow>) -> CLQResult<TypedGraph> {
        let mut source_labels: HashSet<NodeLabel> = HashSet::new();
        let mut target_labels: HashSet<NodeLabel> = HashSet::new();
        let mut target_type_ids: HashMap<NodeLabel, NodeTypeId> = HashMap::new();
        for r in rows.iter() {
            assert!(self.graph_id == r.graph_id);
            source_labels.insert(r.source_id);
            target_labels.insert(r.target_id);
            target_type_ids.insert(r.target_id, r.target_type_id);
        }

        // warrant a canonical order on the label vectors
        let mut source_labels_vec: Vec<NodeLabel> = source_labels.into_iter().collect();
        source_labels_vec.sort();
        let mut target_labels_vec: Vec<NodeLabel> = target_labels.into_iter().collect();
        target_labels_vec.sort();

        let (mut node_map, labels_map, source_ids_vec, target_ids_vec) =
            Self::init_nodes(&source_labels_vec, &target_labels_vec, &target_type_ids);
        Self::populate_edges(&rows, &mut node_map, &labels_map)?;
        let mut graph = Self::create_graph(node_map, source_ids_vec, target_ids_vec, labels_map)?;
        if let Some(min_degree) = self.min_degree {
            graph = Self::prune(graph, &rows, min_degree)?;
        }
        Ok(graph)
    }
}

pub trait TypedGraphBuilderBase {
    fn create_graph(
        nodes: FxHashMap<u32, Node>,
        core_ids: Vec<u32>,
        non_core_ids: Vec<u32>,
        labels_map: FxHashMap<NodeLabel, u32>,
    ) -> CLQResult<TypedGraph> {
        Ok(TypedGraph {
            nodes,
            core_ids,
            non_core_ids,
            labels_map,
        })
    }

    /// given a set of initialized Nodes, populates the respective neighbors fields
    /// appropriately.
    fn populate_edges(
        rows: &[EdgeRow],
        node_map: &mut FxHashMap<u32, Node>,
        labels_map: &FxHashMap<NodeLabel, u32>,
    ) -> CLQResult<()> {
        for r in rows.iter() {
            let source_id: u32 = *labels_map
                .get(&r.source_id)
                .ok_or_else(CLQError::err_none)?;
            let target_id: u32 = *labels_map
                .get(&r.target_id)
                .ok_or_else(CLQError::err_none)?;

            assert!(node_map.contains_key(&source_id));
            assert!(node_map.contains_key(&target_id));

            let source_node = node_map
                .get_mut(&source_id)
                .ok_or_else(CLQError::err_none)?;

            source_node
                .neighbors_sets
                .entry(r.edge_type_id)
                .or_default()
                .insert(target_id);

            source_node
                .edges
                .push(NodeEdge::new(r.edge_type_id, target_id));

            // edges with the same source and target type should not be repeated
            if r.source_type_id != r.target_type_id {
                let target_node = node_map
                    .get_mut(&target_id)
                    .ok_or_else(CLQError::err_none)?;

                target_node
                    .neighbors_sets
                    .entry(r.edge_type_id)
                    .or_default()
                    .insert(source_id);

                target_node
                    .edges
                    .push(NodeEdge::new(r.edge_type_id, source_id));
            }
        }
        Ok(())
    }

    // initializes nodes in the graph with empty neighbors fields.
    // at this point, we convert node ids to internal ids.
    fn init_nodes(
        core_ids: &[NodeLabel],
        non_core_ids: &[NodeLabel],
        non_core_type_ids: &HashMap<NodeLabel, NodeTypeId>,
    ) -> (
        FxHashMap<u32, Node>,
        FxHashMap<NodeLabel, u32>,
        Vec<u32>,
        Vec<u32>,
    ) {
        // returns node_map, label_map, core indexes, non core indexes
        let mut i: u32 = 0;
        let mut node_map: FxHashMap<u32, Node> = FxHashMap::default();
        let mut labels_map: FxHashMap<NodeLabel, u32> = FxHashMap::default();
        let mut core_idxs: Vec<u32> = Vec::with_capacity(core_ids.len());
        let mut non_core_idxs: Vec<u32> = Vec::with_capacity(non_core_ids.len());
        for &id in core_ids {
            let node = Node::new(
                i,              // node_id,
                true,           // is_core,
                None,           // non_core_type,
                Vec::new(),     // edges,
                HashMap::new(), //neighbors
            );
            node_map.insert(i, node);
            core_idxs.push(i);
            labels_map.insert(id, i);
            i += 1;
        }
        for &id in non_core_ids {
            let node = Node::new(
                i,                            // node_id,
                false,                        // is_core,
                Some(non_core_type_ids[&id]), // non_core_type,
                Vec::new(),                   // edges,
                HashMap::new(),               // neighbors
            );
            node_map.insert(i, node);
            labels_map.insert(id, i);
            non_core_idxs.push(i);
            i += 1;
        }
        (node_map, labels_map, core_idxs, non_core_idxs)
    }

    /// Trims edges greedily, until all edges in the graph have degree at least min_degree.
    /// Note that this function does not delete any nodes -- just finds nodes to delete. It is
    /// called by `prune`, which actually does the deletion.
    fn trim_edges(node_map: &mut FxHashMap<u32, Node>, min_degree: &usize) -> HashSet<u32> {
        let mut degree_map: HashMap<u32, usize> = HashMap::new();
        for (node_id, node) in node_map.iter() {
            let node_degree: usize = node.degree();
            degree_map.insert(*node_id, node_degree);
        }
        let mut nodes_to_delete: HashSet<u32> = HashSet::new();
        loop {
            let mut nodes_to_update: HashSet<u32> = HashSet::new();
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
                    let neighbor_node_id: u32 = n.target_id;
                    let current_degree: usize = degree_map[&neighbor_node_id];
                    degree_map.insert(neighbor_node_id, current_degree - 1);
                }
            }
        }
        nodes_to_delete
    }

    /// Takes an already-built graph and the edge rows used to create it, returning a
    /// new graph, where all nodes are assured to have degree at least min_degree.
    /// The provision of a <Self as GraphBuilderBase>::GraphType is necessary, since the notion of "degree" does
    /// not make sense outside of a graph.
    fn prune(graph: TypedGraph, rows: &[EdgeRow], min_degree: usize) -> CLQResult<TypedGraph> {
        let mut target_type_ids: HashMap<NodeLabel, NodeTypeId> = HashMap::new();
        for r in rows.iter() {
            target_type_ids.insert(r.target_id, r.target_type_id);
        }
        let (filtered_source_labels, filtered_target_labels, filtered_rows) =
            Self::get_filtered_sources_targets_rows(graph, min_degree, rows);
        let (mut filtered_node_map, filtered_label_map, filtered_source_ids, filtered_target_ids) =
            Self::init_nodes(
                &filtered_source_labels,
                &filtered_target_labels,
                &target_type_ids,
            );
        Self::populate_edges(&filtered_rows, &mut filtered_node_map, &filtered_label_map)?;
        Self::create_graph(
            filtered_node_map,
            filtered_source_ids,
            filtered_target_ids,
            filtered_label_map,
        )
    }
    /// called by `prune`, finds source and target nodes to exclude, as well as edges to exclude
    /// when rebuilding the graph from a filtered vector of `EdgeRows`.
    fn get_filtered_sources_targets_rows(
        mut graph: TypedGraph,
        min_degree: usize,
        rows: &[EdgeRow],
    ) -> (Vec<NodeLabel>, Vec<NodeLabel>, Vec<EdgeRow>) {
        let exclude_nodes: HashSet<u32> = Self::trim_edges(graph.get_mut_nodes(), &min_degree);
        let filtered_source_ids: Vec<NodeLabel> = graph
            .get_core_labels()
            .iter()
            .filter(|x| !exclude_nodes.contains(&graph.labels_map[x]))
            .cloned()
            .collect();
        let filtered_target_ids: Vec<NodeLabel> = graph
            .get_non_core_labels()
            .unwrap()
            .iter()
            .filter(|x| !exclude_nodes.contains(&graph.labels_map[x]))
            .cloned()
            .collect();
        let filtered_rows: Vec<EdgeRow> = rows
            .iter()
            .filter(|x| {
                !(exclude_nodes.contains(&graph.labels_map[&x.source_id])
                    || (exclude_nodes.contains(&graph.labels_map[&x.target_id])))
            })
            .cloned()
            .collect();
        // todo: make member of struct
        (filtered_source_ids, filtered_target_ids, filtered_rows)
    }
}
impl TypedGraphBuilderBase for TypedGraphBuilder {}
impl GraphBuilderBaseWithPreProcessing for TypedGraphBuilder {}

pub struct TypedGraphBuilderWithCliques {
    pub graph_id: GraphId,
    pub cliques: Vec<(BTreeSet<u32>, BTreeSet<u32>)>,
    pub core_type_id: NodeTypeId,
    pub non_core_type_map: HashMap<u32, NodeTypeId>,
    pub edge_type_map: HashMap<(NodeTypeId, NodeTypeId), Vec<EdgeTypeId>>,
}
impl TypedGraphBuilderBase for TypedGraphBuilderWithCliques {}
impl GraphBuilderBase for TypedGraphBuilderWithCliques {
    type GraphType = TypedGraph;
    type RowType = EdgeRow;

    fn from_vector(&mut self, data: Vec<EdgeRow>) -> CLQResult<TypedGraph> {
        let mut source_labels: HashSet<NodeLabel> = HashSet::new();
        let mut target_labels: HashSet<NodeLabel> = HashSet::new();
        let mut target_type_ids: HashMap<NodeLabel, NodeTypeId> = HashMap::new();
        for r in data.iter() {
            assert!(self.graph_id == r.graph_id);
            source_labels.insert(r.source_id);
            target_labels.insert(r.target_id);
            target_type_ids.insert(r.target_id, r.target_type_id);
        }

        // warrant a canonical order on the id vectors
        let mut source_labels_vec: Vec<NodeLabel> = source_labels.into_iter().collect();
        source_labels_vec.sort();
        let mut target_labels_vec: Vec<NodeLabel> = target_labels.into_iter().collect();
        target_labels_vec.sort();

        let (mut node_map, labels_map, source_ids, target_ids) =
            Self::init_nodes(&source_labels_vec, &target_labels_vec, &target_type_ids);
        Self::populate_edges(&data, &mut node_map, &labels_map)?;
        let graph = Self::create_graph(node_map, source_ids, target_ids, labels_map)?;
        Ok(graph)
    }
}
// impl GraphBuilderBaseWithPreProcessing for TypedGraphBuilderWithCliques {
//     fn pre_process_rows(
//         &mut self,
//         data: Vec<<Self as GraphBuilderBase>::RowType>,
//     ) -> CLQResult<Vec<<Self as GraphBuilderBase>::RowType>> {
//         let mut row_set: HashSet<<Self as GraphBuilderBase>::RowType> = HashSet::new();
//         for el in data.into_iter() {
//             let target_type = el.target_type_id;
//             let edge_type = el.edge_type_id;
//             self.non_core_type_map.insert(el.source_id, target_type);
//             self.edge_type_map
//                 .entry((self.core_type_id, target_type))
//                 .or_insert_with(Vec::new)
//                 .push(edge_type);
//             row_set.insert(el);
//         }

//         for (core, non_core) in self.get_cliques() {
//             for core_id in core {
//                 for non_core_id in non_core {
//                     for clique_edge in self
//                         .get_clique_edges(*core_id, *non_core_id)
//                         .unwrap()
//                         .into_iter()
//                     {
//                         row_set.insert(clique_edge);
//                     }
//                 }
//             }
//         }
//         let rows_with_cliques: Vec<_> = row_set.into_iter().collect();
//         self.non_core_type_map.clear();
//         Ok(rows_with_cliques)
//     }
// }

// impl GraphBuilderBaseWithCliques for TypedGraphBuilderWithCliques {
//     type CliquesType = (BTreeSet<u32>, BTreeSet<u32>);
//     type NodeIdType = u32;

//     fn get_clique_edges(&self, id1: u32, id2: u32) -> CLQResult<Vec<EdgeRow>> {
//         let source_type_id = self.core_type_id;
//         let target_type_id = *self
//             .non_core_type_map
//             .get(&id2)
//             .ok_or_else(CLQError::err_none)?;
//         Ok(self
//             .edge_type_map
//             .get(&(source_type_id, target_type_id))
//             .ok_or_else(CLQError::err_none)?
//             .iter()
//             .cloned()
//             .map(|edge_type_id| EdgeRow {
//                 graph_id: self.graph_id,
//                 source_id: id1,
//                 target_id: id2,
//                 source_type_id: self.core_type_id,
//                 target_type_id,
//                 edge_type_id,
//             })
//             .collect())
//     }
//     fn get_cliques(&self) -> &Vec<(BTreeSet<u32>, BTreeSet<u32>)> {
//         &self.cliques
//     }
// }
