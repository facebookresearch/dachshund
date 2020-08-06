/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
// see https://stackoverflow.com/questions/36088116/how-to-do-polymorphic-io-from-either-a-file-or-stdin-in-rust
use std::fs::File;
use std::io::{self, BufRead, Read};
use std::os::unix::io::FromRawFd;
pub struct Input<'a> {
    source: Box<dyn BufRead + 'a>,
}

impl<'a> Input<'a> {
    pub fn console(_stdin: &'a io::Stdin) -> Input<'a> {
        let stdin = unsafe { File::from_raw_fd(0) };
        let reader = io::BufReader::new(stdin);
        Input {
            source: Box::new(reader),
        }
    }

    pub fn file(path: &str) -> io::Result<Input<'a>> {
        File::open(path).map(|file| Input {
            source: Box::new(io::BufReader::new(file)),
        })
    }

    pub fn string(text: &'a [u8]) -> Input<'a> {
        Input {
            source: Box::new(text),
        }
    }
}

impl<'a> Read for Input<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.source.read(buf)
    }
}

impl<'a> BufRead for Input<'a> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.source.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.source.consume(amt);
    }
}
