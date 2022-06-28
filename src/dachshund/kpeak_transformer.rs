/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate serde_json;

use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_builder_base::GraphBuilderBase;
use crate::dachshund::id_types::{GraphId, NodeId};
use crate::dachshund::line_processor::{LineProcessor, LineProcessorBase};
use crate::dachshund::row::{Row, SimpleEdgeRow};
use crate::dachshund::simple_transformer::GraphStatsTransformerBase;
use crate::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use crate::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use crate::dachshund::transformer_base::TransformerBase;
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use crate::dachshund::algorithms::k_peaks::KPeaks;

pub struct KPeakTransformer {
    batch: Vec<SimpleEdgeRow>,
    line_processor: Arc<LineProcessor>,
}

impl KPeakTransformer {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            line_processor: Arc::new(LineProcessor::new()),
        }
    }
    fn compute_kpeaks_and_mountains(
        graph: &SimpleUndirectedGraph,
    ) -> (HashMap<NodeId, i32>, HashMap<usize, HashMap<NodeId, usize>>) {
        graph.get_k_peak_mountain_assignment()
    }
}
impl Default for KPeakTransformer {
    fn default() -> Self {
        KPeakTransformer::new()
    }
}
impl GraphStatsTransformerBase for KPeakTransformer {}

impl TransformerBase for KPeakTransformer {
    fn get_line_processor(&self) -> Arc<dyn LineProcessorBase> {
        self.line_processor.clone()
    }
    fn process_row(&mut self, row: Box<dyn Row>) -> CLQResult<()> {
        self.batch.push(row.as_simple_edge_row().unwrap());
        Ok(())
    }
    fn reset(&mut self) -> CLQResult<()> {
        self.batch.clear();
        Ok(())
    }

    fn process_batch(
        &mut self,
        graph_id: GraphId,
        output: &Sender<(Option<String>, bool)>,
    ) -> CLQResult<()> {
        let tuples: Vec<(i64, i64)> = self.batch.iter().map(|x| x.as_tuple()).collect();
        let mut builder = SimpleUndirectedGraphBuilder {};
        let graph = builder.from_vector(tuples)?;
        let (peaks, mountain_assignments) = KPeakTransformer::compute_kpeaks_and_mountains(&graph);
        let original_id = self
            .line_processor
            .get_original_id(graph_id.value() as usize);
        for (m_id, m_nodes) in mountain_assignments {
            for (n_id, coreness) in m_nodes {
                let k_peak = *peaks.get(&n_id).unwrap();
                let line: String = format!(
                    "{}\t{}\t{}\t{}\t{}",
                    original_id,
                    n_id.value(),
                    coreness,
                    k_peak,
                    m_id
                );
                output.send((Some(line), false)).unwrap();
            }
        }
        Ok(())
    }
}
