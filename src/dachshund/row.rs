/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::id_types::{EdgeTypeId, GraphId, NodeId, NodeTypeId};
use std::fmt;

///  Used to keep track of edge row input.
#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub struct EdgeRow {
    pub graph_id: GraphId,
    pub source_id: NodeId,
    pub target_id: NodeId,
    pub source_type_id: NodeTypeId,
    pub target_type_id: NodeTypeId,
    pub edge_type_id: EdgeTypeId,
}
impl fmt::Display for EdgeRow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EdgeRow: {}\t{}\t{}",
            self.graph_id, self.source_id, self.target_id
        )
    }
}
/// used to keep track of clique row input (when used for initialization of search
/// algorithm) or output (when used to output results of search algorithm).
#[derive(Copy, Clone)]
pub struct CliqueRow {
    pub graph_id: GraphId,
    pub node_id: NodeId,
    // is None when node is source
    pub target_type: Option<NodeTypeId>,
}
impl CliqueRow {
    pub fn new<T: Into<NodeId>>(
        graph_id: GraphId,
        node_id: T,
        target_type: Option<NodeTypeId>,
    ) -> Self {
        Self {
            graph_id,
            node_id: node_id.into(),
            target_type,
        }
    }
}

/// used to keep track of row input for simple graphs.
#[derive(Copy, Clone)]
pub struct SimpleEdgeRow {
    pub graph_id: GraphId,
    pub source_id: NodeId,
    pub target_id: NodeId,
}
impl SimpleEdgeRow {
    pub fn as_tuple(&self) -> (i64, i64) {
        (self.source_id.value(), self.target_id.value())
    }
}
/// Used in lieu of a union type. All rows processed by a Transformer
/// must implement this trait.
pub trait Row {
    /// this is the key used by each transformer.
    fn get_graph_id(&self) -> GraphId;
    fn as_edge_row(&self) -> Option<EdgeRow>;
    fn as_clique_row(&self) -> Option<CliqueRow>;
    fn as_simple_edge_row(&self) -> Option<SimpleEdgeRow>;
}
impl Row for EdgeRow {
    fn get_graph_id(&self) -> GraphId {
        self.graph_id
    }
    fn as_edge_row(&self) -> Option<EdgeRow> {
        Some(*self)
    }
    fn as_clique_row(&self) -> Option<CliqueRow> {
        None
    }
    fn as_simple_edge_row(&self) -> Option<SimpleEdgeRow> {
        None
    }
}
impl Row for CliqueRow {
    fn get_graph_id(&self) -> GraphId {
        self.graph_id
    }
    fn as_edge_row(&self) -> Option<EdgeRow> {
        None
    }
    fn as_clique_row(&self) -> Option<CliqueRow> {
        Some(*self)
    }
    fn as_simple_edge_row(&self) -> Option<SimpleEdgeRow> {
        None
    }
}
impl Row for SimpleEdgeRow {
    fn get_graph_id(&self) -> GraphId {
        self.graph_id
    }
    fn as_edge_row(&self) -> Option<EdgeRow> {
        None
    }
    fn as_clique_row(&self) -> Option<CliqueRow> {
        None
    }
    fn as_simple_edge_row(&self) -> Option<SimpleEdgeRow> {
        Some(*self)
    }
}
