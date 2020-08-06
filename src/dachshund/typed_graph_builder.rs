/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_builder::GraphBuilder;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::Node;
use crate::dachshund::typed_graph::TypedGraph;
use std::collections::HashMap;

pub struct TypedGraphBuilder {}
impl GraphBuilder<TypedGraph> for TypedGraphBuilder {
    fn _new(
        nodes: HashMap<NodeId, Node>,
        core_ids: Vec<NodeId>,
        non_core_ids: Vec<NodeId>,
    ) -> CLQResult<TypedGraph> {
        Ok(TypedGraph {
            nodes,
            core_ids,
            non_core_ids,
        })
    }
}
