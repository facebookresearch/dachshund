/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate serde_json;

use crate::dachshund::algorithms::coreness::Coreness;
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

pub struct CoreTransformer {
    batch: Vec<SimpleEdgeRow>,
    line_processor: Arc<LineProcessor>,
}

impl CoreTransformer {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            line_processor: Arc::new(LineProcessor::new()),
        }
    }
    fn compute_coreness_and_anomalies(
        graph: &SimpleUndirectedGraph,
    ) -> (HashMap<NodeId, usize>, HashMap<NodeId, f64>) {
        let (_, coreness) = graph.get_coreness();
        let coreness_anomalies = graph.get_coreness_anomaly(&coreness);
        (coreness, coreness_anomalies)
    }
}
impl Default for CoreTransformer {
    fn default() -> Self {
        CoreTransformer::new()
    }
}
impl GraphStatsTransformerBase for CoreTransformer {}

impl TransformerBase for CoreTransformer {
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
        let (coreness_map, anomaly_map) = CoreTransformer::compute_coreness_and_anomalies(&graph);
        let original_id = self
            .line_processor
            .get_original_id(graph_id.value() as usize);
        let mut corenesses: Vec<(NodeId, usize)> = coreness_map.into_iter().collect();
        corenesses.sort_by_key(|(_node_id, coreness)| *coreness);
        for (node_id, coreness) in corenesses {
            let degree = graph.get_node_degree(node_id);
            let anomaly = anomaly_map.get(&node_id).unwrap();
            let line: String = format!(
                "{}\t{}\t{}\t{}\t{}",
                original_id,
                node_id.value(),
                coreness,
                degree,
                anomaly
            );
            output.send((Some(line), false)).unwrap();
        }
        Ok(())
    }
}
