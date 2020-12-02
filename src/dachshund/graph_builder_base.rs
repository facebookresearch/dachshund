/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_base::GraphBase;

pub trait GraphBuilderBase
where
    Self: Sized,
    Self::GraphType: GraphBase,
{
    type GraphType;
    type RowType;
    fn from_vector(&self, data: &Vec<Self::RowType>) -> CLQResult<Self::GraphType>;
}
