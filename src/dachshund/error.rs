/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
// https://blog.burntsushi.net/rust-error-handling/

use thiserror::Error;

pub type CLQResult<T> = std::result::Result<T, CLQError>;

#[derive(Debug, Error)]
pub enum CLQError {
    #[error("{0}")]
    Generic(String),

    #[error("I/O Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Bad UTF8 in string: {0}")]
    UTF8(#[from] std::string::FromUtf8Error),

    #[error("Parse error: {0}")]
    ParseBool(#[from] std::str::ParseBoolError),

    #[error("Parse error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("Parse error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("JSON error: {0}")]
    JSON(#[from] serde_json::Error),

    #[error("Impossible error: {0}")]
    Infallible(#[from] std::convert::Infallible),
}

impl CLQError {
    pub fn new(msg: &str) -> Self {
        Self::Generic(msg.to_owned())
    }
    pub fn err_none() -> Self {
        Self::Generic("Unexpectedly empty Option encountered.".to_owned())
    }
}

impl From<String> for CLQError {
    fn from(str: String) -> Self {
        CLQError::Generic(str)
    }
}

impl From<&str> for CLQError {
    fn from(str: &str) -> Self {
        CLQError::Generic(str.to_owned())
    }
}
