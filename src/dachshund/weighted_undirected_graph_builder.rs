/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_builder_base::{GraphBuilderBase, GraphBuilderBaseWithPreProcessing};
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{WeightedNode, WeightedNodeEdge};
use crate::dachshund::weighted_undirected_graph::WeightedUndirectedGraph;
use std::collections::BTreeMap;
extern crate fxhash;
use fxhash::FxHashMap;

pub struct WeightedUndirectedGraphBuilder {}

pub trait TWeightedUndirectedGraphBuilder:
    GraphBuilderBase<GraphType = WeightedUndirectedGraph, RowType = (i64, i64, f64)>
{
    fn get_node_ids(data: &Vec<(i64, i64, f64)>) -> BTreeMap<NodeId, BTreeMap<NodeId, f64>> {
        let mut ids: BTreeMap<NodeId, BTreeMap<NodeId, f64>> = BTreeMap::new();
        for (id1, id2, weight) in data {
            ids.entry(NodeId::from(*id1))
                .or_insert_with(BTreeMap::new)
                .insert(NodeId::from(*id2), *weight);
            ids.entry(NodeId::from(*id2))
                .or_insert_with(BTreeMap::new)
                .insert(NodeId::from(*id1), *weight);
        }
        ids
    }
    fn get_nodes(ids: BTreeMap<NodeId, BTreeMap<NodeId, f64>>) -> FxHashMap<NodeId, WeightedNode> {
        let mut nodes: FxHashMap<NodeId, WeightedNode> = FxHashMap::default();
        for (id, neighbors) in ids.into_iter() {
            nodes.insert(
                id,
                WeightedNode {
                    node_id: id,
                    edges: neighbors
                        .iter()
                        .map(|(target_id, weight)| WeightedNodeEdge {
                            target_id: *target_id,
                            weight: *weight,
                        })
                        .collect(),
                    neighbors: neighbors.keys().cloned().collect(),
                },
            );
        }
        nodes
    }
}

impl TWeightedUndirectedGraphBuilder for WeightedUndirectedGraphBuilder {}
impl GraphBuilderBaseWithPreProcessing for WeightedUndirectedGraphBuilder {}
impl GraphBuilderBase for WeightedUndirectedGraphBuilder {
    type GraphType = WeightedUndirectedGraph;
    type RowType = (i64, i64, f64);

    // builds a graph from a vector of IDs. Repeated edges are ignored.
    // Edges only need to be provided once (this being an undirected graph)
    #[allow(clippy::ptr_arg)]
    fn from_vector(&mut self, data: Vec<(i64, i64, f64)>) -> CLQResult<WeightedUndirectedGraph> {
        let rows = self.pre_process_rows(data)?;
        let ids = Self::get_node_ids(&rows);
        let nodes = Self::get_nodes(ids);
        Ok(WeightedUndirectedGraph {
            ids: nodes.keys().cloned().collect(),
            nodes,
        })
    }
}
