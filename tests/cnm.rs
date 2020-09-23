/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;
use crate::lib_dachshund::TransformerBase;
use lib_dachshund::dachshund::id_types::NodeId;
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::simple_transformer::{
    GraphStatsTransformerBase, SimpleParallelTransformer, SimpleTransformer,
};
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;
use std::collections::{BTreeSet, HashSet};
use std::iter::FromIterator;

fn get_graph(idx: usize) -> Result<SimpleUndirectedGraph, String> {
    let v = match idx {
        0 => vec![(0, 1), (1, 2), (2, 0)],
        _ => return Err("Invalid index".to_string()),
    };
    Ok(SimpleUndirectedGraphBuilder::from_vector(
        &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    ))
}

#[cfg(test)]
#[test]
fn test_triad_cnm() {
    let g = get_graph(0).unwrap();
    let (mut communities, mut degree_map, mut delta_q_bmap,
         mut delta_q_maxheap, mut H, num_edges) = g.init_cnm_communities();
    assert_eq!(communities.len(), 3);
    assert_eq!(degree_map.len(), 3);
    assert_eq!(delta_q_bmap.len(), 3);
    assert_eq!(delta_q_maxheap.len(), 3);
    assert_eq!(H.len(), 3);
    assert_eq!(num_edges, 3);

    assert_eq!(degree_map[&0], 2);
    assert_eq!(degree_map[&1], 2);
    assert_eq!(degree_map[&2], 2);

    let (delta_ij, i, j) = H.peek().unwrap();
    assert_eq!(*delta_ij, 1.0 / 6.0 - (2.0 * 2.0) / 36.0);
    assert_eq!(*i, 2);
    assert_eq!(*j, 1);

    let (mut communities, mut degree_map, mut delta_q_bmap,
         mut delta_q_maxheap, mut H, num_edges) = g.iterate_cnm_communities(
        communities, degree_map, delta_q_bmap, delta_q_maxheap, H, num_edges
    );
    assert_eq!(communities.len(), 2);
    assert_eq!(degree_map.len(), 2);
    assert_eq!(delta_q_bmap.len(), 2);
    assert_eq!(delta_q_maxheap.len(), 2);
    assert_eq!(H.len(), 2);
    assert_eq!(num_edges, 3);

    assert_eq!(degree_map[&1], 4);
    assert_eq!(degree_map[&0], 2);
    let (delta_ij, i, j) = H.peek().unwrap();
    assert_eq!(*delta_ij, 2.0 * (1.0 / 6.0 - (2.0 * 2.0) / 36.0));
    assert_eq!(*i, 1);
    assert_eq!(*j, 0);

    let (communities, degree_map, delta_q_bmap,
         delta_q_maxheap, H, num_edges) = g.iterate_cnm_communities(
        communities, degree_map, delta_q_bmap, delta_q_maxheap, H, num_edges
    );
    assert_eq!(communities.len(), 1);
    assert_eq!(degree_map.len(), 1);
    assert_eq!(delta_q_bmap.len(), 1);
    assert_eq!(delta_q_maxheap.len(), 1);
    // H drops down to 0 at this point
    assert_eq!(H.len(), 0);
    assert_eq!(num_edges, 3);

    assert_eq!(degree_map[&0], 6);
}
