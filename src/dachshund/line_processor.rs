/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate clap;
extern crate serde_json;

use crate::dachshund::error::CLQResult;
use crate::dachshund::id_types::{GraphId, NodeId};
use crate::dachshund::row::{Row, SimpleEdgeRow};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// deals with processing lines and turning them into rows.
/// Can mutate ids and reverse_ids maps that keep track of
/// graph_ids seen so far.
pub struct LineProcessor {
    ids: Arc<RwLock<HashMap<String, i64>>>,
    reverse_ids: Arc<RwLock<Vec<String>>>,
}
impl LineProcessor {
    pub fn new() -> Self {
        Self {
            ids: Arc::new(RwLock::new(HashMap::new())),
            reverse_ids: Arc::new(RwLock::new(Vec::new())),
        }
    }
    fn record_new_key_or_return_current_one(&self, key: String) -> GraphId {
        let mut ids = self.ids.write().unwrap();
        let mut reverse_ids = self.reverse_ids.write().unwrap();
        let num_items: usize = ids.len();
        if !ids.contains_key(&key) {
            ids.insert(key.clone(), num_items as i64);
            reverse_ids.push(key.clone());
        }
        let id = ids.get(&key).unwrap();
        GraphId::from(*id)
    }
    pub fn process_line(&self, line: String) -> CLQResult<Box<dyn Row>> {
        let vec: Vec<&str> = line.split('\t').collect();
        assert!(vec.len() == 3);
        let key = vec[0].to_string();
        let graph_id = self.record_new_key_or_return_current_one(key);
        let source_id: NodeId = vec[1].parse::<i64>()?.into();
        let target_id: NodeId = vec[2].parse::<i64>()?.into();
        Ok(Box::new(SimpleEdgeRow {
            graph_id,
            source_id,
            target_id,
        }))
    }
    pub fn get_original_id(&self, local_id: usize) -> String {
        self.reverse_ids.read().unwrap()[local_id].clone()
    }
}
impl Default for LineProcessor {
    fn default() -> Self {
        LineProcessor::new()
    }
}
