/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this core tree.
 */
extern crate clap;
extern crate serde_json;

use std::collections::HashMap;
use std::io::prelude::*;

use clap::ArgMatches;

use crate::dachshund::beam::{Beam, BeamSearchResult};
use crate::dachshund::error::{CLQError, CLQResult};
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::graph_builder::GraphBuilder;
use crate::dachshund::id_types::{EdgeTypeId, GraphId, NodeId, NodeTypeId};
use crate::dachshund::input::Input;
use crate::dachshund::output::Output;
use crate::dachshund::row::{CliqueRow, EdgeRow, Row};

/// A mapping from opaque strings identifying node types (e.g. "author"), to the associated integer
/// identifier used internally. Encapsulates some special/convenient accessor/mutator logic.
pub struct NonCoreTypeIds {
    data: HashMap<String, NodeTypeId>,
}

impl NonCoreTypeIds {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn require(&self, type_str: &str) -> CLQResult<&NodeTypeId> {
        let id = self
            .data
            .get(type_str)
            .ok_or_else(|| CLQError::from(format!("No mapping for non-core type: {}", type_str)))?;
        Ok(id)
    }
    fn require_mut(&mut self, type_str: &str) -> CLQResult<&mut NodeTypeId> {
        let id = self
            .data
            .get_mut(type_str)
            .ok_or_else(|| CLQError::from(format!("No mapping for non-core type: {}", type_str)))?;
        Ok(id)
    }
    fn insert(&mut self, type_str: &str, type_id: NodeTypeId) {
        if !self.data.contains_key(type_str) {
            self.data.insert(type_str.to_owned(), type_id);
        }
    }

    pub fn type_name(&self, non_core_type_id: &NodeTypeId) -> Option<String> {
        self.data.iter().find_map(|(k, v)| {
            if v == non_core_type_id {
                Some(k.to_owned())
            } else {
                None
            }
        })
    }
}

/// Used to set up the typed graph clique mining algorithm.
pub struct Transformer {
    pub core_type: String,
    pub non_core_type_ids: NonCoreTypeIds,
    pub non_core_types: Vec<String>,
    pub edge_types: Vec<String>,
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
        let mut edge_types: Vec<String> = typespec.iter().map(|x| x[1].clone()).collect();
        edge_types.sort();

        let mut non_core_types: Vec<String> = typespec.iter().map(|x| x[2].clone()).collect();
        non_core_types.sort();

        let num_non_core_types: usize = non_core_types.len();
        let non_core_type_ids: NonCoreTypeIds =
            Transformer::process_typespec(typespec, &core_type, non_core_types.to_vec())?;
        let transformer = Self {
            core_type,
            non_core_type_ids,
            non_core_types,
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

    /// processes a line of (tab-separated) input, of the form:
    /// graph_id\tcore_id\tnon_core_id\tcore_type\tedge_type\tnon_core_type
    ///
    /// or:
    ///
    /// graph_id\tnode_id\tnode_type
    ///
    /// Note that core_type is not used in the first row type. The second
    /// row type is used to initialize the beam search with a single existing
    /// clique, the best identified by some other search process. This existing
    /// clique may be invalidated if it no longer meets cliqueness requirements
    /// as per the current search process.
    pub fn process_line(&self, line: String) -> CLQResult<Box<dyn Row>> {
        let vec: Vec<&str> = line.split('\t').collect();
        // this is an edge row if we have something on column 3
        assert!(vec.len() == 6);
        let is_edge_row: bool = !vec[3].is_empty();
        if is_edge_row {
            let graph_id: GraphId = vec[0].parse::<i64>()?.into();
            let core_id: NodeId = vec[1].parse::<i64>()?.into();
            let non_core_id: NodeId = vec[2].parse::<i64>()?.into();
            let edge_type: &str = vec[4].trim_end();
            let non_core_type: &str = vec[5].trim_end();
            let non_core_type_id: NodeTypeId = *self.non_core_type_ids.require(non_core_type)?;
            let edge_type_id: EdgeTypeId = self
                .edge_types
                .iter()
                .position(|r| r == edge_type)
                .ok_or_else(CLQError::err_none)?
                .into();
            let core_type_id: NodeTypeId = *self.non_core_type_ids.require(&self.core_type)?;
            return Ok(Box::new(EdgeRow {
                graph_id,
                source_id: core_id,
                target_id: non_core_id,
                source_type_id: core_type_id,
                target_type_id: non_core_type_id,
                edge_type_id,
            }));
        }
        let graph_id: GraphId = vec[0].parse::<i64>()?.into();
        let node_id: NodeId = vec[1].parse::<i64>()?.into();
        let node_type: &str = vec[2].trim_end();
        let non_core_type: Option<NodeTypeId>;
        if node_type == self.core_type {
            non_core_type = None;
        } else {
            let non_core_type_id: NodeTypeId = *self.non_core_type_ids.require(node_type)?;
            non_core_type = Some(non_core_type_id);
        }
        Ok(Box::new(CliqueRow {
            graph_id,
            node_id,
            target_type: non_core_type,
        }))
    }
    /// Given a properly-built graph, runs the quasi-clique detection beam search on it.
    pub fn process_graph<'a, TGraph: GraphBase>(
        &'a self,
        graph: &'a TGraph,
        clique_rows: Vec<CliqueRow>,
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
        clique_rows: Vec<CliqueRow>,
        graph_id: GraphId,
        verbose: bool,
        output: &mut Output,
    ) -> CLQResult<Option<BeamSearchResult<'a, TGraph>>> {
        if graph.get_core_ids().is_empty() || graph.get_non_core_ids().unwrap().is_empty() {
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
                output.print(line)?;
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
    /// to be called by main.rs (or a test), using an input (such as stdin),
    /// which must provide a lines() function, and an output (such as stdout), to
    /// which it is possible to write line-by-line. Typical reducer logic:
    /// read one line at a time, with the first column being the key. If key has not
    /// changed, keep accumulating lines. If key has changed, process accumulated
    /// lines, output results and reset state.  
    pub fn run<TGraphBuilder: GraphBuilder<TGraph>, TGraph: GraphBase>(
        &self,
        input: Input,
        output: &mut Output,
    ) -> CLQResult<()> {
        let mut edge_rows: Vec<EdgeRow> = Vec::new();
        let mut clique_rows: Vec<CliqueRow> = Vec::new();
        let mut current_graph_id: Option<GraphId> = None;

        for line in input.lines() {
            match line {
                Ok(n) => {
                    let raw: Box<dyn Row> = self.process_line(n)?;
                    let new_graph_id: GraphId = raw.get_graph_id();
                    if let Some(current_id) = current_graph_id {
                        if new_graph_id != current_id {
                            let graph: TGraph = self.build_pruned_graph::<TGraphBuilder, TGraph>(
                                current_id, &edge_rows,
                            )?;
                            self.process_clique_rows::<TGraphBuilder, TGraph>(
                                &graph,
                                clique_rows,
                                current_id,
                                // verbose
                                self.debug,
                                output,
                            )?;
                            edge_rows = Vec::new();
                            clique_rows = Vec::new();
                        }
                    }
                    current_graph_id = Some(new_graph_id);
                    if let Some(r) = raw.as_edge_row() {
                        edge_rows.push(r)
                    }
                    if let Some(r) = raw.as_clique_row() {
                        clique_rows.push(r)
                    }
                }
                Err(error) => eprintln!("I/O error: {}", error),
            }
        }
        if let Some(current_id) = current_graph_id {
            let graph: TGraph =
                self.build_pruned_graph::<TGraphBuilder, TGraph>(current_id, &edge_rows)?;
            self.process_clique_rows::<TGraphBuilder, TGraph>(
                &graph,
                clique_rows,
                current_id,
                // verbose
                self.debug,
                output,
            )?;
            return Ok(());
        }
        Err("No input rows!".into())
    }
}
