/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
#![feature(map_first_last)]
extern crate clap;
extern crate lib_dachshund;

use std::io;

use clap::{App, Arg, ArgMatches};

use lib_dachshund::dachshund::error::CLQResult;
use lib_dachshund::dachshund::graph::{TypedGraph, TypedGraphBuilder};
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::transformer::Transformer;

fn get_command_line_args() -> ArgMatches<'static> {
    let matches: ArgMatches = App::new("Dachshund")
        .version("0.1.0")
        .author("
                Alex Peysakhovich <alexpeys@fb.com>, \
                Bogdan State <bogdanstate@fb.com>, \
                Julian Mestre <julianmestre@fb.com>, \
                Michael Chen <mvc@fb.com>,
                Matthew Menard <mlmenard@fb.com>,
                Pär Winzell <zell@fb.com>")
        .about("Finds (quasi-)bicliques in graphs specified from stdin.")
        .arg(Arg::with_name("typespec")
                 .short("ts")
                 .long("typespec")
                 .takes_value(true)
                 .help("JSON-encoded array of arrays representing Dachshund types. E.g.: \
                       [[\"author\", \"works_at\", \"university\"], [\"author\", \"published_in\", \"journal\"]]"))
        .arg(Arg::with_name("beam_size")
                 .short("b")
                 .long("beam_size")
                 .takes_value(true)
                 .help("Beam size (number of candidates considered at any point in the search"))
        .arg(Arg::with_name("alpha")
                 .short("a")
                 .long("alpha")
                 .takes_value(true)
                 .help("Alpha ('cliqueness weight') used to indicate how much to weigh global \
                       Beam size (number of candidates considered at any point in the search"))
        .arg(Arg::with_name("global_thresh")
                 .short("g")
                 .long("global_thresh")
                 .takes_value(true)
                 .help("Global density threshold: min % of ties out of all possible ties \
                        required for a clique to be considered valid for the purposes of \
                        the search."))
        .arg(Arg::with_name("local_thresh")
                 .short("l")
                 .long("local_thresh")
                 .takes_value(true)
                 .help("Local density threshold: min % of ties out of all possible ties \
                        required for each node, in order for a clique to be considered \
                        valid for the purposes of the search."))
        .arg(Arg::with_name("num_to_search")
                 .short("n")
                 .long("num_to_search")
                 .takes_value(true)
                 .help("Number of candidate nodes to consider (and score) for \
                        each existing clique in the beam. Candidate nodes are ordered in \
                        decreasing order of the # of ties to nodes currently in candidate."))
        .arg(Arg::with_name("epochs")
                 .short("e")
                 .long("epochs")
                 .takes_value(true)
                 .help("Number of epochs for which to run each search"))
        .arg(Arg::with_name("max_repeated_prior_scores")
                 .short("m")
                 .long("max_repeated_prior_scores")
                 .takes_value(true)
                 .help("Number of times for which the top prior score, if repeated, would trigger an early \
                        stop in the search process."))
        .arg(Arg::with_name("debug_mode")
                 .short("d")
                 .long("debug_mode")
                 .takes_value(true)
                 .help("Whether to run in debug mode (printing lots of useful messages about \
                        candidates (default = false)."))
        .arg(Arg::with_name("long_format")
                 .long("long_format")
                 .takes_value(true)
                 .help("Whether to print clique assignments in long format: \
                        clique_id\tnode_id\tnode_type \
                        (default = false)"))
        .arg(Arg::with_name("core_type")
                 .long("core_type")
                 .takes_value(true)
                 .help("What the type of the core entity is"))
        .arg(Arg::with_name("min_degree")
                 .long("min_degree")
                 .takes_value(true)
                 .help("Min degree for each node in each clique (nodes are pruned iteratively until \
                        all candidate nodes have at least this degree w/r to all other nodes in the \
                        graph"))
        .get_matches();
    matches
}

fn main() -> CLQResult<()> {
    let matches: ArgMatches = get_command_line_args();
    let transformer = Transformer::from_argmatches(matches)?;
    let stdio: io::Stdin = io::stdin();
    let input: Input = Input::console(&stdio);
    let mut dummy: Vec<u8> = Vec::new();
    let mut output: Output = Output::console(&mut dummy);
    transformer.run::<TypedGraphBuilder, TypedGraph>(input, &mut output)?;
    Ok(())
}
