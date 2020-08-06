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

use clap::{App, ArgMatches};

use lib_dachshund::dachshund::error::CLQResult;
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::simple_transformer::SimpleTransformer;
use lib_dachshund::dachshund::simple_transformer::TransformerBase;

fn get_command_line_args() -> ArgMatches<'static> {
    let matches: ArgMatches = App::new("Dachshund Graph Featurizer")
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
        .about("Featurizes simple undirected graphs specified from stdin.")
        .get_matches();
    matches
}

fn main() -> CLQResult<()> {
    // TODO: add proper command line args
    let _matches: ArgMatches = get_command_line_args();
    let mut transformer = SimpleTransformer::new();
    let stdio: io::Stdin = io::stdin();
    let input: Input = Input::console(&stdio);
    let mut dummy: Vec<u8> = Vec::new();
    let output: Output = Output::console(&mut dummy);
    transformer.run(input, output)?;
    Ok(())
}
