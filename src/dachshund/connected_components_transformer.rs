/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate fxhash;
extern crate serde_json;

use crate::dachshund::algorithms::connected_components::ConnectedComponentsUndirected;
use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_builder_base::GraphBuilderBase;
use crate::dachshund::line_processor::{LineProcessor, LineProcessorBase};
use crate::dachshund::row::{Row, SimpleEdgeRow};
use crate::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use crate::dachshund::transformer_base::TransformerBase;
use crate::GraphId;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub struct ConnectedComponentsTransformer {
    batch: Vec<SimpleEdgeRow>,
    line_processor: Arc<LineProcessor>,
}
impl ConnectedComponentsTransformer {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            line_processor: Arc::new(LineProcessor::new()),
        }
    }
}
impl Default for ConnectedComponentsTransformer {
    fn default() -> Self {
        ConnectedComponentsTransformer::new()
    }
}

impl TransformerBase for ConnectedComponentsTransformer {
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
        
        let conn_comp = graph.get_connected_components();
        let original_id = self
            .line_processor
            .get_original_id(graph_id.value() as usize);
        for (cid, nodes) in conn_comp.into_iter().enumerate() {
            for node_id in nodes {
                let line = format!("{}\t{}\t{}", original_id, cid, node_id.value());
                output.send((Some(line), false)).unwrap();
            }
        }
        Ok(())
    }
}
