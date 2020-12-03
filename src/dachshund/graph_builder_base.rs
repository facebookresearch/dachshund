/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_base::GraphBase;
pub trait GraphBuilderBase
where
    Self: Sized,
    Self::GraphType: GraphBase,
{
    type GraphType;

    fn from_vector(data: &Vec<(i64, i64)>) -> Self::GraphType;
}
