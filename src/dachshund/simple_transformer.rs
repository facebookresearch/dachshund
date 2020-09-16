/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate serde_json;

use crate::dachshund::error::CLQResult;
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::{GraphId, NodeId};
use crate::dachshund::line_processor::{LineProcessor, LineProcessorBase};
use crate::dachshund::row::{Row, SimpleEdgeRow};
use crate::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use crate::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use crate::dachshund::transformer_base::TransformerBase;
use rand::seq::SliceRandom;
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde_json::json;
use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub struct SimpleTransformer {
    batch: Vec<SimpleEdgeRow>,
    line_processor: Arc<LineProcessor>,
}
pub struct SimpleParallelTransformer {
    batch: Vec<SimpleEdgeRow>,
    pool: ThreadPool,
    line_processor: Arc<LineProcessor>,
}
pub trait GraphStatsTransformerBase: TransformerBase {
    fn compute_graph_stats_json(graph: &SimpleUndirectedGraph) -> String {
        let conn_comp = graph.get_connected_components();
        let largest_cc = conn_comp.iter().max_by_key(|x| x.len()).unwrap();
        let sources: Vec<NodeId> = largest_cc
            .choose_multiple(&mut rand::thread_rng(), 100)
            .copied()
            .collect();
        let betcent = graph
            .get_node_betweenness_starting_from_sources(&sources, false, Some(&largest_cc))
            .unwrap();
        let evcent = graph.get_eigenvector_centrality(0.001, 1000);

        let mut removed: HashSet<NodeId> = HashSet::new();
        let k_cores_2 = graph._get_k_cores(2, &mut removed);
        let k_trusses_3 = graph._get_k_trusses(3, &removed).1;
        let k_cores_4 = graph._get_k_cores(4, &mut removed);
        let k_trusses_5 = graph._get_k_trusses(5, &removed).1;
        let k_cores_8 = graph._get_k_cores(8, &mut removed);
        let k_trusses_9 = graph._get_k_trusses(9, &removed).1;
        let k_cores_16 = graph._get_k_cores(16, &mut removed);
        let k_trusses_17 = graph._get_k_trusses(17, &removed).1;

        json!({
            "num_edges": graph.count_edges(),
            "num_2_cores": k_cores_2.len(),
            "num_4_cores": k_cores_4.len(),
            "num_8_cores": k_cores_8.len(),
            "num_16_cores": k_cores_16.len(),
            "num_3_trusses": k_trusses_3.len(),
            "num_5_trusses": k_trusses_5.len(),
            "num_9_trusses": k_trusses_9.len(),
            "num_17_trusses": k_trusses_17.len(),
            "num_connected_components": conn_comp.len(),
            "size_of_largest_cc": largest_cc.len(),
            "bet_cent": (Iterator::sum::<f64>(betcent.values()) /
                (betcent.len() as f64) * 1000.0).floor() / 1000.0,
            "evcent": (Iterator::sum::<f64>(evcent.values()) /
                (evcent.len() as f64) * 1000.0).floor() / 1000.0,
            "clust_coef": (graph.get_avg_clustering() * 1000.0).floor() / 1000.0,
        })
        .to_string()
    }
}
impl SimpleTransformer {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            line_processor: Arc::new(LineProcessor::new()),
        }
    }
}
impl Default for SimpleTransformer {
    fn default() -> Self {
        SimpleTransformer::new()
    }
}
impl SimpleParallelTransformer {
    pub fn new() -> Self {
        Self {
            batch: Vec::new(),
            line_processor: Arc::new(LineProcessor::new()),
            pool: ThreadPoolBuilder::new().build().unwrap(),
        }
    }
}
impl Default for SimpleParallelTransformer {
    fn default() -> Self {
        SimpleParallelTransformer::new()
    }
}

impl TransformerBase for SimpleTransformer {
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
    fn process_batch(&self, graph_id: GraphId,
                     output: &Sender<(Option<String>, bool)>) -> CLQResult<()> {
        let tuples: Vec<(i64, i64)> = self.batch.iter().map(|x| x.as_tuple()).collect();
        let graph = SimpleUndirectedGraphBuilder::from_vector(&tuples);
        let stats = Self::compute_graph_stats_json(&graph);
        let original_id = self
            .line_processor
            .get_original_id(graph_id.value() as usize);
        let line: String = format!("{}\t{}", original_id, stats);
        output.send((Some(line), false)).unwrap();
        Ok(())
    }
}
impl TransformerBase for SimpleParallelTransformer {
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
    fn process_batch(&self, graph_id: GraphId, output: &Sender<(Option<String>, bool)>) -> CLQResult<()> {
        let tuples: Vec<(i64, i64)> = self.batch.iter().map(|x| x.as_tuple()).collect();
        let output_clone = output.clone();
        let line_processor = self.line_processor.clone();
        self.pool.spawn(move || {
            let graph = SimpleUndirectedGraphBuilder::from_vector(&tuples);
            let stats = Self::compute_graph_stats_json(&graph);
            let original_id = line_processor.get_original_id(graph_id.value() as usize);
            let line: String = format!("{}\t{}", original_id, stats);
            output_clone.send((Some(line), false)).unwrap();
        });
        Ok(())
    }
}
impl GraphStatsTransformerBase for SimpleTransformer {}
impl GraphStatsTransformerBase for SimpleParallelTransformer {}
