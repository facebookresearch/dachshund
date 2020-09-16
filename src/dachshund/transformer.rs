/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this core tree.
 */
extern crate clap;
extern crate serde_json;

use clap::ArgMatches;

use crate::dachshund::beam::{Beam, BeamSearchResult};
use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::graph_builder::GraphBuilder;
use crate::dachshund::id_types::{GraphId, NodeTypeId};
use crate::dachshund::line_processor::LineProcessorBase;
use crate::dachshund::non_core_type_ids::NonCoreTypeIds;
use crate::dachshund::row::{CliqueRow, EdgeRow, Row};
use crate::dachshund::transformer_base::TransformerBase;
use crate::dachshund::typed_graph::TypedGraph;
use crate::dachshund::typed_graph_builder::TypedGraphBuilder;
use crate::dachshund::typed_graph_line_processor::TypedGraphLineProcessor;
use std::rc::Rc;
use std::sync::mpsc::Sender;
use std::sync::Arc;

/// Used to set up the typed graph clique mining algorithm.
pub struct Transformer {
    pub core_type: String,
    pub non_core_type_ids: Rc<NonCoreTypeIds>,
    pub non_core_types: Rc<Vec<String>>,
    pub line_processor: Arc<TypedGraphLineProcessor>,
    pub edge_types: Rc<Vec<String>>,
    pub beam_size: usize,
    pub alpha: f32,
    pub global_thresh: Option<f32>,
    pub local_thresh: Option<f32>,
    pub num_to_search: usize,
    pub num_epochs: usize,
    pub max_repeated_prior_scores: usize,
    pub num_non_core_types: usize,
    pub debug: bool,
    pub min_degree: usize,
    pub long_format: bool,

    edge_rows: Vec<EdgeRow>,
    clique_rows: Vec<CliqueRow>,
}
impl TransformerBase for Transformer {
    fn get_line_processor(&self) -> Arc<dyn LineProcessorBase> {
        self.line_processor.clone()
    }
    fn process_row(&mut self, row: Box<dyn Row>) -> CLQResult<()> {
        if let Some(edge_row) = row.as_edge_row() {
            self.edge_rows.push(edge_row);
        }
        if let Some(clique_row) = row.as_clique_row() {
            self.clique_rows.push(clique_row);
        }
        Ok(())
    }
    fn reset(&mut self) -> CLQResult<()> {
        self.edge_rows.clear();
        self.clique_rows.clear();
        Ok(())
    }
    fn process_batch(&self, graph_id: GraphId, output: &Sender<(Option<String>, bool)>) -> CLQResult<()> {
        let graph: TypedGraph =
            self.build_pruned_graph::<TypedGraphBuilder, TypedGraph>(graph_id, &self.edge_rows)?;
        self.process_clique_rows::<TypedGraphBuilder, TypedGraph>(
            &graph,
            &self.clique_rows,
            graph_id,
            // verbose
            self.debug,
            output,
        )?;
        Ok(())
    }
}
impl Transformer {
    /// processes a "typespec", a command-line argument, of the form:
    /// [["author", "published_in", "journal"], ["author", "co-authored", "article"]].
    /// This sets up the semantics related to the set of relations contained in the
    /// typed graph. A requirement is that all relations share a "core" type, in this
    /// case, "author". Non-core types must be listed in a vector, which is used to
    /// index the non core-types. The function creates a vector of NonCoreTypeIds, which
    /// will then be used to process input rows.
    pub fn process_typespec(
        typespec: Vec<Vec<String>>,
        core_type: &str,
        non_core_types: Vec<String>,
    ) -> CLQResult<NonCoreTypeIds> {
        let mut non_core_type_ids = NonCoreTypeIds::new();
        non_core_type_ids.insert(core_type, NodeTypeId::from(0 as usize));

        let should_be_only_this_core_type = &typespec[0][0].clone();
        for (non_core_type_ix, non_core_type) in non_core_types.iter().enumerate() {
            non_core_type_ids.insert(&non_core_type, NodeTypeId::from(non_core_type_ix + 1));
        }
        for item in typespec {
            let core_type = &item[0];
            let non_core_type = &item[2];
            assert_eq!(core_type, should_be_only_this_core_type);
            let non_core_type_id: &mut NodeTypeId = non_core_type_ids.require_mut(non_core_type)?;
            non_core_type_id.increment_possible_edge_count();
        }
        Ok(non_core_type_ids)
    }
    /// Called by main.rs module to set up the beam search. Parameters are as follows:
    ///     - `typespec`: a command-line argument, of the form:
    ///     [["author", "published_in", "journal"], ["author", "co-authored", "article"]].
    ///     This sets up the semantics related to the set of relations contained in the
    ///     typed graph. A requirement is that all relations share a "core" type, in this
    ///     case, "author".
    ///     - `beam_size`: Beam construction parameter. The number of top candidates to
    ///     maintain as potential future cores for expansion in the "beam" (i.e., the list of top candidates).
    ///     - `alpha`: `Scorer` constructor parameter. Controls the contribution of density
    ///     - `global_thresh`: `Scorer` constructor parameter. If provided, candidates must be at
    ///     least this dense to be considered valid (quasi-)cliques.
    ///     - `local_thresh`: `Scorer` constructor parameter. if provided, each node in the candidate
    ///     must have at least `local_thresh` proportion of ties to other nodes in the candidate,
    ///     for the candidate to be considered valid.
    ///     - `num_to_search`: number of expansion candidates to consider for each candidate in the
    ///     beam.
    ///     - `num_epochs`: maximum number of epochs to run search for.
    ///     - `max_repeated_prior_scores`: maximum number of times for which the top score can be
    ///     repeated in consecutive epochs, before the search gets shut down early.
    ///     - `debug`: whether to produce verbose output in the search process.
    ///     - `min_degree`: minimum degree required for each node in a (quasi-)clique in order for
    ///     the subgraph to be considered interesting.
    ///     - `core_type`: the core type, as found in the typespec.
    ///     - `long_format`: whether to output results in long format, of the form:
    ///     `graph_id\tnode_id\tnode_type`, instead of the more user-friendly (but
    ///     machine-unfriendly) wide format.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        typespec: Vec<Vec<String>>,
        beam_size: usize,
        alpha: f32,
        global_thresh: Option<f32>,
        local_thresh: Option<f32>,
        num_to_search: usize,
        num_epochs: usize,
        max_repeated_prior_scores: usize,
        debug: bool,
        min_degree: usize,
        core_type: String,
        long_format: bool,
    ) -> CLQResult<Self> {
        let mut edge_types_v: Vec<String> = typespec.iter().map(|x| x[1].clone()).collect();
        edge_types_v.sort();
        let edge_types = Rc::new(edge_types_v);

        let mut non_core_types_v: Vec<String> = typespec.iter().map(|x| x[2].clone()).collect();
        non_core_types_v.sort();
        let non_core_types = Rc::new(non_core_types_v);

        let num_non_core_types: usize = non_core_types.len();
        let non_core_type_ids: Rc<NonCoreTypeIds> = Rc::new(Transformer::process_typespec(
            typespec,
            &core_type,
            non_core_types.to_vec(),
        )?);
        let line_processor = Arc::new(TypedGraphLineProcessor::new(
            core_type.clone(),
            non_core_type_ids.clone(),
            non_core_types.clone(),
            edge_types.clone(),
        ));
        let transformer = Self {
            core_type,
            non_core_type_ids,
            non_core_types,
            line_processor,
            edge_types,
            beam_size,
            alpha,
            global_thresh,
            local_thresh,
            num_to_search,
            num_epochs,
            max_repeated_prior_scores,
            num_non_core_types,
            debug,
            min_degree,
            long_format,
            edge_rows: Vec::new(),
            clique_rows: Vec::new(),
        };
        Ok(transformer)
    }

    /// constructs a transformer from an ArgMatches object (to help with command line arguments).
    pub fn from_argmatches(matches: ArgMatches) -> CLQResult<Self> {
        let arg_value = |name: &str| -> CLQResult<&str> {
            matches
                .value_of(name)
                .ok_or_else(|| CLQError::from(format!("Missing required argument: {}", name)))
        };
        let typespec_str: &str = arg_value("typespec")?;
        let typespec: Vec<Vec<String>> = serde_json::from_str(typespec_str)?;
        let beam_size: usize = arg_value("beam_size")?.parse::<usize>()?;
        let alpha: f32 = arg_value("alpha")?.parse::<f32>()?;
        let global_thresh: Option<f32> = Some(arg_value("global_thresh")?.parse::<f32>()?);
        let local_thresh: Option<f32> = Some(arg_value("local_thresh")?.parse::<f32>()?);
        let num_to_search: usize = arg_value("num_to_search")?.parse::<usize>()?;
        let num_epochs: usize = arg_value("epochs")?.parse::<usize>()?;
        let max_repeated_prior_scores: usize =
            arg_value("max_repeated_prior_scores")?.parse::<usize>()?;
        let debug: bool = arg_value("debug_mode")?.parse::<bool>()?;
        let min_degree: usize = arg_value("min_degree")?.parse::<usize>()?;
        let core_type: String = arg_value("core_type")?.parse::<String>()?;
        let long_format: bool = arg_value("long_format")?.parse::<bool>()?;
        let transformer = Transformer::new(
            typespec,
            beam_size,
            alpha,
            global_thresh,
            local_thresh,
            num_to_search,
            num_epochs,
            max_repeated_prior_scores,
            debug,
            min_degree,
            core_type,
            long_format,
        )?;
        Ok(transformer)
    }

    /// builds graph, pruned to ensure all nodes have at least self.min_degree degree
    /// with other nodes in the graph. This is done via a greedy algorithm which removes
    /// low-degree nodes iteratively.
    #[allow(clippy::ptr_arg)]
    pub fn build_pruned_graph<TGraphBuilder: GraphBuilder<TGraph>, TGraph: GraphBase>(
        &self,
        graph_id: GraphId,
        rows: &Vec<EdgeRow>,
    ) -> CLQResult<TGraph> {
        TGraphBuilder::new(graph_id, rows, Some(self.min_degree))
    }

    /// Given a properly-built graph, runs the quasi-clique detection beam search on it.
    pub fn process_graph<'a, TGraph: GraphBase>(
        &'a self,
        graph: &'a TGraph,
        clique_rows: &'a Vec<CliqueRow>,
        graph_id: GraphId,
        verbose: bool,
    ) -> CLQResult<BeamSearchResult<'a, TGraph>> {
        let mut beam: Beam<TGraph> = Beam::new(
            graph,
            clique_rows,
            self.beam_size,
            verbose,
            &self.non_core_types,
            self.num_non_core_types,
            self.alpha,
            self.global_thresh,
            self.local_thresh,
            graph_id,
        )?;
        beam.run_search(
            self.num_to_search,
            self.beam_size,
            self.num_epochs,
            self.max_repeated_prior_scores,
        )
    }
    /// Used to "seed" the beam search with an existing best (quasi-)clique (if any provided),
    /// and then run the search under the parameters specified in the constructor.
    pub fn process_clique_rows<'a, TGraphBuilder: GraphBuilder<TGraph>, TGraph: GraphBase>(
        &'a self,
        graph: &'a TGraph,
        clique_rows: &'a Vec<CliqueRow>,
        graph_id: GraphId,
        verbose: bool,
        output: &Sender<(Option<String>, bool)>,
    ) -> CLQResult<Option<BeamSearchResult<'a, TGraph>>> {
        if graph.get_core_ids().is_empty() || graph.get_non_core_ids().unwrap().is_empty() {
            // still have to send an acknowledgement to the output channel
            // that we have actually processed this graph, otherwise
            // we lose track of how many graphs have been processed so
            // far!
            output.send((None, false)).unwrap();
            return Ok(None);
        }
        let result: BeamSearchResult<TGraph> =
            self.process_graph(graph, clique_rows, graph_id, verbose)?;
        // only print if this is a conforming clique
        if result.top_candidate.get_score()? > 0.0 {
            if !self.long_format {
                let line: String = format!(
                    "{}\t{}",
                    graph_id.value(),
                    result
                        .top_candidate
                        .to_printable_row(&self.non_core_types)?,
                );
                output.send((Some(line), false)).unwrap();
            } else {
                result.top_candidate.print(
                    graph_id,
                    &self.non_core_types,
                    &self.core_type,
                    output,
                )?;
            }
        }
        Ok(Some(result))
    }
}
