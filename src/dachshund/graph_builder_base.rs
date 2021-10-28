/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use std::hash::Hash;

pub trait GraphBuilderBaseWithPreProcessing: GraphBuilderBase {
    fn pre_process_rows(
        &mut self,
        data: Vec<<Self as GraphBuilderBase>::RowType>,
    ) -> CLQResult<Vec<<Self as GraphBuilderBase>::RowType>> {
        Ok(data)
    }
}

pub trait GraphBuilderBase
where
    Self: Sized,
    Self::GraphType: GraphBase,
{
    type GraphType;
    type RowType;
    fn from_vector(&mut self, data: Vec<Self::RowType>) -> CLQResult<Self::GraphType>;
}

pub trait GraphBuilderBaseWithCliques: GraphBuilderBaseWithPreProcessing
where
    <Self as GraphBuilderBase>::RowType: Eq,
    <Self as GraphBuilderBase>::RowType: Hash,
{
    type CliquesType;

    fn get_clique_edges(
        &self,
        id1: NodeId,
        id2: NodeId,
    ) -> CLQResult<Vec<<Self as GraphBuilderBase>::RowType>>;
    fn get_cliques(&self) -> &Vec<Self::CliquesType>;
}
