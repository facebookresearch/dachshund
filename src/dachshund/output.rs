/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
// see https://stackoverflow.com/questions/36088116/how-to-do-polymorphic-io-from-either-a-file-or-stdin-in-rust
use std::io::Error;
use std::io::Write;

use crate::dachshund::error::CLQResult;

pub struct Output<'a> {
    pub destination: &'a mut Vec<u8>,
    is_stdout: bool,
}

impl<'a> Output<'a> {
    pub fn console(text: &'a mut Vec<u8>) -> Output<'a> {
        Output {
            destination: text,
            is_stdout: true,
        }
    }
    pub fn string(text: &'a mut Vec<u8>) -> Output {
        Output {
            destination: text,
            is_stdout: false,
        }
    }
    pub fn print(&mut self, text: String) -> CLQResult<()> {
        if !self.is_stdout {
            self.write_all(text.as_bytes())?;
            self.write_all(b"\n")?;
            self.flush()?;
            return Ok(());
        }
        println!("{}", text);
        Ok(())
    }
}
impl<'a> Write for Output<'a> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        self.destination.write(buf)
    }

    fn flush(&mut self) -> Result<(), Error> {
        self.destination.flush()
    }
}
