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
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::connected_components_transformer::ConnectedComponentsTransformer;
use lib_dachshund::dachshund::strongly_connected_components_transformer::StronglyConnectedComponentsTransformer;
use lib_dachshund::dachshund::transformer_base::TransformerBase;

fn get_command_line_args() -> ArgMatches<'static> {
    let matches: ArgMatches = App::new("Dachshund Connected Components")
        .version("0.0.1")
        .author(
            "
                Alex Peysakhovich <alexpeys@fb.com>, \
                Bogdan State <bogdanstate@fb.com>, \
                Julian Mestre <julianmestre@fb.com>, \
                Michael Chen <mvc@fb.com>,
                Matthew Menard <mlmenard@fb.com>,
                Pär Winzell <zell@fb.com>",
        )
        .about("Takes in graphs, extracts connected components.")
        .arg(
            Arg::with_name("directed")
                .short("d")
                .help("Interpret input as directed graph and calculate strongly connected components."),
        )
        .get_matches();
    matches
}

fn main() -> CLQResult<()> {

    let matches: ArgMatches = get_command_line_args();
    let stdio: io::Stdin = io::stdin();
    let input: Input = Input::console(&stdio);
    let mut dummy: Vec<u8> = Vec::new();
    let output: Output = Output::console(&mut dummy);
    if matches.is_present("directed") {
        ConnectedComponentsTransformer::new().run(input, output)?;
    } else {
        StronglyConnectedComponentsTransformer::new().run(input, output)?;
    };
    Ok(())
}
