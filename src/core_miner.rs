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

use lib_dachshund::dachshund::core_transformer::CoreTransformer;
use lib_dachshund::dachshund::error::CLQResult;
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::kpeak_transformer::KPeakTransformer;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::transformer_base::TransformerBase;
use lib_dachshund::dachshund::weighted_core_transformer::WeightedCoreTransformer;

fn get_command_line_args() -> ArgMatches<'static> {
    let matches: ArgMatches = App::new("Dachshund Core Miner")
        .version("0.0.2")
        .author(
            "
                Alex Peysakhovich <alexpeys@fb.com>, \
                Bogdan State <bogdanstate@fb.com>, \
                Julian Mestre <julianmestre@fb.com>, \
                Michael Chen <mvc@fb.com>,
                Matthew Menard <mlmenard@fb.com>,
                Pär Winzell <zell@fb.com>,
                Anushka Mehta <anushkamehta@fb.com>",
        )
        .about("Calculates (weighted) coreness values in graphs from stdin.")
        .arg(
            Arg::with_name("weighted")
                .short("w")
                .help("Calculate weighted version of k-cores (requires edge weights in input)."),
        )
        .arg(
            Arg::with_name("kpeaks")
                .long("kpeaks")
                .help("Calculates k-peak values and mountain assignments in graphs from stdin."),
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
    assert!(
        !(matches.is_present("weighted") && matches.is_present("kpeaks")),
        "Input arguments include kpeaks and weighted. Cannot run kpeaks on weighted graph."
    );
    if matches.is_present("weighted") {
        WeightedCoreTransformer::new().run(input, output)?;
    } else if matches.is_present("kpeaks") {
        KPeakTransformer::new().run(input, output)?;
    } else {
        CoreTransformer::new().run(input, output)?;
    };
    Ok(())
}
