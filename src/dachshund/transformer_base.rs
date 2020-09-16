/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate serde_json;

use crate::dachshund::error::CLQResult;
use crate::dachshund::id_types::GraphId;
use crate::dachshund::input::Input;
use crate::dachshund::line_processor::LineProcessorBase;
use crate::dachshund::output::Output;
use crate::dachshund::row::Row;
use std::io::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub trait TransformerBase {
    fn get_line_processor(&self) -> Arc<dyn LineProcessorBase>;
    // logic for taking row and storing into self via side-effect
    fn process_row(&mut self, row: Box<dyn Row>) -> CLQResult<()>;
    // logic for processing batch of rows, once all rows are ready
    fn process_batch(&self, graph_id: GraphId, output: &Sender<(Option<String>, bool)>) -> CLQResult<()>;
    // reset transformer state after processing;
    fn reset(&mut self) -> CLQResult<()>;

    // main loop, runs through lines ordered by graph_id, updates state accordingly
    // and runs process_batch when graph_id changes
    fn run(&mut self, input: Input, mut output: Output) -> CLQResult<()> {
        let ret = crossbeam::scope(|scope| {
            let line_processor = self.get_line_processor();
            let num_processed = Arc::new(AtomicUsize::new(0 as usize));
            let (sender, receiver) = channel();
            let num_processed_clone = num_processed.clone();
            let writer = scope.spawn(move |_| loop {
                match receiver.recv() {
                    Ok((line, shutdown)) => {
                        if shutdown {
                            return;
                        }
                        if let Some(string) = line {
                            output.print(string).unwrap();
                        }
                        num_processed_clone.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(error) => panic!(error),
                }
            });
            let mut current_graph_id: Option<GraphId> = None;
            let mut num_to_process: usize = 0;
            for line in input.lines() {
                match line {
                    Ok(n) => {
                        let row: Box<dyn Row> = line_processor.process_line(n)?;
                        let new_graph_id: GraphId = row.get_graph_id();
                        if let Some(some_current_graph_id) = current_graph_id {
                            if new_graph_id != some_current_graph_id {
                                self.process_batch(some_current_graph_id, &sender.clone())?;
                                num_to_process += 1;
                                self.reset()?;
                            }
                        }
                        current_graph_id = Some(new_graph_id);
                        self.process_row(row)?;
                    }
                    Err(error) => eprintln!("I/O error: {}", error),
                }
            }
            if let Some(some_current_graph_id) = current_graph_id {
                self.process_batch(some_current_graph_id, &sender)?;
                num_to_process += 1;
                while num_to_process > num_processed.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(100));
                }
                sender.send((None, true)).unwrap();
                writer.join().unwrap();
                return Ok(());
            }
            Err("No input rows!".into())
        });
        ret.unwrap()
    }
}
