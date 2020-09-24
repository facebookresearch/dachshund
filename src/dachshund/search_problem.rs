/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
pub struct SearchProblem {
    pub beam_size: usize,
    pub alpha: f32,
    pub global_thresh: Option<f32>,
    pub local_thresh: Option<f32>,
    pub num_to_search: usize,
    pub num_epochs: usize,
    pub max_repeated_prior_scores: usize,
    pub min_degree: usize,
}
impl SearchProblem {
    pub fn new(
        beam_size: usize,
        alpha: f32,
        global_thresh: Option<f32>,
        local_thresh: Option<f32>,
        num_to_search: usize,
        num_epochs: usize,
        max_repeated_prior_scores: usize,
        min_degree: usize,
    ) -> Self {
        Self {
            beam_size,
            alpha,
            global_thresh,
            local_thresh,
            num_to_search,
            num_epochs,
            max_repeated_prior_scores,
            min_degree,
        }
    }
}
