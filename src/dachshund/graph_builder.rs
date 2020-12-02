/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate fxhash;
extern crate nalgebra as na;
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::Node;
use crate::dachshund::row::EdgeRow;
use fxhash::FxHashMap;

/// Trait encapsulting the logic required to build a graph from a set of edge
/// rows. Currently used to build typed graphs.
pub trait GraphBuilder<TGraph: GraphBase>
where
    Self: Sized,
    TGraph: Sized,
    TGraph: GraphBase<NodeType = Node>,
{
    fn create_graph(
        nodes: FxHashMap<NodeId, Node>,
        core_ids: Vec<NodeId>,
        non_core_ids: Vec<NodeId>,
    ) -> CLQResult<TGraph>;

    /// given a set of initialized Nodes, populates the respective neighbors fields
    /// appropriately.
    fn populate_edges(rows: &[EdgeRow], node_map: &mut FxHashMap<NodeId, Node>) -> CLQResult<()>;
    
}
