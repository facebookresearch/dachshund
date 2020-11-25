/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;
use lib_dachshund::dachshund::algorithms::cnm_communities::CNMCommunities;
use lib_dachshund::dachshund::simple_undirected_graph::SimpleUndirectedGraph;
use lib_dachshund::dachshund::simple_undirected_graph_builder::SimpleUndirectedGraphBuilder;

fn get_graph(idx: usize) -> Result<SimpleUndirectedGraph, String> {
    let v = match idx {
        0 => vec![(0, 1), (1, 2), (2, 0)],
        1 => vec![(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3)],
        2 => vec![(0, 1), (1, 2), (2, 0), (0, 3)],
        3 => vec![
            (0, 1),
            (1, 2),
            (2, 0),
            (0, 3),
            (1, 4),
            (2, 5),
            (4, 5),
            (1, 6),
        ],
        _ => return Err("Invalid index".to_string()),
    };
    Ok(SimpleUndirectedGraphBuilder::from_vector(
        &v.into_iter().map(|(x, y)| (x as i64, y as i64)).collect(),
    ))
}
fn get_expected_modularity_changes(idx: usize) -> Result<Vec<f64>, String> {
    match idx {
        3 => Ok(vec![0.1015625, 0.09375, 0.09375, 0.03125]),
        _ => return Err("Invalid index".to_string()),
    }
}

#[cfg(test)]
#[test]
fn test_triad_cnm_iter() {
    let g = get_graph(0).unwrap();
    let x = g.init_cnm_communities();
    assert_eq!(x.communities.len(), 3);
    assert_eq!(x.degree_map.len(), 3);
    assert_eq!(x.delta_q_bmap.len(), 3);
    assert_eq!(x.delta_q_maxheap.len(), 3);
    assert_eq!(x.maxh.len(), 3);
    assert_eq!(x.num_edges, 3);

    assert_eq!(x.degree_map[&0], 2);
    assert_eq!(x.degree_map[&1], 2);
    assert_eq!(x.degree_map[&2], 2);

    let (delta_ij, i, j) = x.maxh.peek().unwrap().tuple();
    assert_eq!(delta_ij, 2.0 * (1.0 / 6.0 - (2.0 * 2.0) / 36.0));
    assert_eq!(i, 0);
    assert_eq!(j, 1);

    let x = g.iterate_cnm_communities(x);
    assert_eq!(x.communities.len(), 2);
    assert_eq!(x.degree_map.len(), 2);
    assert_eq!(x.delta_q_bmap.len(), 2);
    assert_eq!(x.delta_q_maxheap.len(), 2);
    assert_eq!(x.maxh.len(), 2);
    assert_eq!(x.num_edges, 3);

    assert_eq!(x.degree_map[&1], 4);
    assert_eq!(x.degree_map[&2], 2);
    let (delta_ij, i, j) = x.maxh.peek().unwrap().tuple();
    assert_eq!(delta_ij, 4.0 * (1.0 / 6.0 - (2.0 * 2.0) / 36.0));
    assert_eq!(i, 1);
    assert_eq!(j, 2);

    let x = g.iterate_cnm_communities(x);
    assert_eq!(x.communities.len(), 1);
    assert_eq!(x.degree_map.len(), 1);
    assert_eq!(x.delta_q_bmap.len(), 1);
    assert_eq!(x.delta_q_maxheap.len(), 1);
    // H drops down to 0 at this point
    assert_eq!(x.maxh.len(), 0);
    assert_eq!(x.num_edges, 3);

    assert_eq!(x.degree_map[&2], 6);
}

#[test]
fn test_triad_cnm_whole() {
    let g = get_graph(0).unwrap();
    let (communities, _) = g.get_cnm_communities();
    assert_eq!(communities.len(), 1);
    assert_eq!(
        communities
            .values()
            .map(|x| x.len())
            .collect::<Vec<usize>>()[0],
        3
    );
}

#[test]
fn test_two_triads_cnm() {
    let g = get_graph(1).unwrap();
    let (communities, _) = g.get_cnm_communities();
    assert_eq!(communities.len(), 2);
    assert_eq!(
        communities
            .values()
            .map(|x| x.len())
            .collect::<Vec<usize>>()[0],
        3
    );
    for k in communities.keys() {
        println!("Key: {}", k);
    }
}

#[test]
fn test_tendril_cnm() {
    let g = get_graph(2).unwrap();
    let x = g.init_cnm_communities();
    assert_eq!(x.communities.len(), 4);
    assert_eq!(x.degree_map.len(), 4);
    assert_eq!(x.delta_q_bmap.len(), 4);
    assert_eq!(x.delta_q_maxheap.len(), 4);
    assert_eq!(x.maxh.len(), 4);
    assert_eq!(x.num_edges, 4);

    let (delta_ij, _i, _j) = x.maxh.peek().unwrap().tuple();
    assert_eq!(delta_ij, 2.0 / 8.0 - 2.0 * (1.0 * 3.0) / 64.0);
}

#[test]
fn test_modularity_changes() {
    let g = get_graph(3).unwrap();
    let (_, modularity_changes) = g.get_cnm_communities();
    let expected = get_expected_modularity_changes(3).unwrap();
    for i in 0..expected.len() {
        assert_eq!(modularity_changes[i], expected[i]);
    }
}
