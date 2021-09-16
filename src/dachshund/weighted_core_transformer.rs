/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate serde_json;
extern crate ordered_float;

use crate::dachshund::algorithms::coreness::FractionalCoreness;
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_builder_base::GraphBuilderBase;
use crate::dachshund::id_types::{GraphId, NodeId};
use crate::dachshund::line_processor::{WeightedLineProcessor, LineProcessorBase};
use crate::dachshund::row::{Row, WeightedEdgeRow};
use crate::dachshund::transformer_base::TransformerBase;
use crate::dachshund::weighted_undirected_graph_builder::WeightedUndirectedGraphBuilder;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use ordered_float::NotNan;

pub struct WeightedCoreTransformer {
    batch: Vec<WeightedEdgeRow>,
    line_processor: Arc<WeightedLineProcessor>,
}

impl WeightedCoreTransformer {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            line_processor: Arc::new(WeightedLineProcessor::new()),
        }
    }
}
impl Default for WeightedCoreTransformer {
    fn default() -> Self {
        WeightedCoreTransformer::new()
    }
}

impl TransformerBase for WeightedCoreTransformer {
    fn get_line_processor(&self) -> Arc<dyn LineProcessorBase> {
        self.line_processor.clone()
    }
    fn process_row(&mut self, row: Box<dyn Row>) -> CLQResult<()> {
        self.batch.push(row.as_weighted_edge_row().unwrap());
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
        let tuples: Vec<(i64, i64, f64)> = self.batch.iter().map(|x| x.as_tuple()).collect();
        let mut builder = WeightedUndirectedGraphBuilder {};
        let graph = builder.from_vector(tuples)?;
        let coreness_map = graph.get_fractional_coreness_values();
        let original_id = self
            .line_processor
            .get_original_id(graph_id.value() as usize);
        let mut coreness: Vec<(NodeId, f64)> = coreness_map.into_iter().collect();
        coreness.sort_by_key(|(_node_id, coreness)| NotNan::new(*coreness).unwrap());
        for (node_id, node_coreness) in coreness {
            let degree = graph.get_node_degree(node_id);
            let line: String = format!(
                "{}\t{}\t{}\t{}",
                original_id,
                node_id.value(),
                node_coreness,
                degree
            );
            output.send((Some(line), false)).unwrap();
        }
        Ok(())
    }
}
