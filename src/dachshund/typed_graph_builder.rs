/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;

use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_builder::GraphBuilder;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{Node, NodeEdge};
use crate::dachshund::row::EdgeRow;
use crate::dachshund::typed_graph::TypedGraph;
use fxhash::FxHashMap;

pub struct TypedGraphBuilder {}
impl GraphBuilder<TypedGraph> for TypedGraphBuilder {

    fn create_graph(
        nodes: FxHashMap<NodeId, Node>,
        core_ids: Vec<NodeId>,
        non_core_ids: Vec<NodeId>,
    ) -> CLQResult<TypedGraph> {
        Ok(TypedGraph {
            nodes,
            core_ids,
            non_core_ids,
        })
    }
    
    /// given a set of initialized Nodes, populates the respective neighbors fields
    /// appropriately.
    fn populate_edges(rows: &[EdgeRow], node_map: &mut FxHashMap<NodeId, Node>) -> CLQResult<()> {
        for r in rows.iter() {
            assert!(node_map.contains_key(&r.source_id));
            assert!(node_map.contains_key(&r.target_id));

            let source_node = node_map
                .get_mut(&r.source_id)
                .ok_or_else(CLQError::err_none)?;

            source_node
                .neighbors
                .entry(r.target_id)
                .or_insert_with(Vec::new);
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

                target_node
                    .neighbors
                    .entry(r.source_id)
                    .or_insert_with(Vec::new);
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
}
