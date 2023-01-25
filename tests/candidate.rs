/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;

use std::collections::{HashMap, HashSet};

use lib_dachshund::dachshund::candidate::{Candidate, Recipe};
use lib_dachshund::dachshund::error::CLQResult;
use lib_dachshund::dachshund::id_types::{GraphId, NodeId};
use lib_dachshund::dachshund::node::Node;
use lib_dachshund::dachshund::row::CliqueRow;
use lib_dachshund::dachshund::row::EdgeRow;
use lib_dachshund::dachshund::scorer::Scorer;
use lib_dachshund::dachshund::test_utils::{gen_test_transformer, process_raw_vector};
use lib_dachshund::dachshund::transformer::Transformer;
use lib_dachshund::dachshund::typed_graph::{LabeledGraph, TypedGraph};

extern crate fxhash;
use fxhash::FxHashMap;

#[cfg(test)]
#[test]
fn test_output_simple_candidate() -> CLQResult<()> {
    let node_id = NodeId::from(0);
    let node_idx = 0;
    let node: Node = Node::new(node_idx, true, None, Vec::new(), HashMap::new());
    let mut graph: TypedGraph = TypedGraph {
        nodes: FxHashMap::default(),
        core_ids: vec![],
        non_core_ids: vec![],
        labels_map: FxHashMap::default(),
    };
    graph.nodes.insert(node_idx, node);
    graph.core_ids.push(node_idx);
    graph.labels_map.insert(node_id, node_idx);

    let mut candidate: Candidate<TypedGraph> = Candidate::init_blank(&graph, 1);
    candidate.add_node(node_idx)?;
    let graph_id: GraphId = 1.into();
    let output_rows: Vec<CliqueRow> =
        candidate.get_output_rows(graph_id, graph.get_reverse_labels_map())?;
    assert_eq!(output_rows.len(), 1);
    assert_eq!(output_rows[0].graph_id, graph_id);
    assert_eq!(output_rows[0].node_id, NodeId::from(0));
    assert_eq!(output_rows[0].target_type, None);
    Ok(())
}

#[test]
fn test_rebuild_candidate() -> CLQResult<()> {
    let typespec: Vec<Vec<String>> = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let raw: Vec<String> = vec!["0\t1\t2\tauthor\tpublished\tarticle".to_string()];
    let graph_id: GraphId = 0.into();

    let transformer: Transformer = gen_test_transformer(typespec, "author".to_string())?;
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw)?;
    let graph: TypedGraph = transformer.build_pruned_graph(graph_id, rows)?;
    assert_eq!(graph.core_ids.len(), 1);
    let core_node_id: u32 = *graph.core_ids.first().unwrap();
    assert_eq!(graph.non_core_ids.len(), 1);
    let non_core_node_id: u32 = *graph.non_core_ids.first().unwrap();

    let scorer: Scorer = Scorer::new(2, &transformer.search_problem);
    let mut candidate: Candidate<TypedGraph> = Candidate::new(core_node_id, &graph, &scorer)?;
    candidate.add_node(non_core_node_id)?;
    let score: f32 = scorer.score(&mut candidate)?;
    candidate.set_score(score)?;

    let graph_id: GraphId = 1.into();
    let output_rows: Vec<CliqueRow> =
        candidate.get_output_rows(graph_id, graph.get_reverse_labels_map())?;
    assert_eq!(output_rows.len(), 2);
    assert_eq!(output_rows[0].graph_id, graph_id);
    assert_eq!(output_rows[0].node_id, NodeId::from(1));
    assert_eq!(output_rows[0].target_type, None);
    let new_candidate = Candidate::from_clique_rows(&output_rows, &graph, &scorer)?.unwrap();
    println!("Candidate: {}", candidate);
    println!("New candidate: {}", new_candidate);
    assert!(candidate.eq(&new_candidate));
    Ok(())
}

/// Build  a toy graph for use with the following tests:
/// (odds are authors, evens are articles);
///
///  1 - 2
///    \\
///  3 - 4
///    \
///  5 - 6
fn build_sample_graph() -> (TypedGraph, Transformer) {
    let typespec: Vec<Vec<String>> = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let raw: Vec<String> = vec![
        "0\t1\t2\tauthor\tpublished\tarticle".to_string(),
        "0\t1\t4\tauthor\tpublished\tarticle".to_string(),
        "0\t1\t4\tauthor\tcited\tarticle".to_string(),
        "0\t3\t4\tauthor\tpublished\tarticle".to_string(),
        "0\t3\t6\tauthor\tpublished\tarticle".to_string(),
        "0\t5\t6\tauthor\tpublished\tarticle".to_string(),
    ];
    let graph_id: GraphId = 0.into();

    let transformer: Transformer = gen_test_transformer(typespec, "author".to_string()).unwrap();
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw).unwrap();
    (
        transformer.build_pruned_graph(graph_id, rows).unwrap(),
        transformer,
    )
}

/// Test that a candidate correctly tracks its neighborhood.
///
///  1 - 2
///    \\
///  3 - 4
///    \
///  5 - 6
///
/// Start with {1}, then add 4, then add 3.
#[test]
fn test_neighborhood() -> CLQResult<()> {
    let (graph, transformer) = build_sample_graph();
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id: u32 = 1;
    let scorer: Scorer = Scorer::new(2, &transformer.search_problem);

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    let node_2: u32 = graph.get_node_by_label(2.into()).node_id;
    let node_3: u32 = graph.get_node_by_label(3.into()).node_id;
    let node_4: u32 = graph.get_node_by_label(4.into()).node_id;
    let node_6: u32 = graph.get_node_by_label(6.into()).node_id;

    let neighborhood = candidate.get_neighborhood();
    let mut expected_neighborhood: HashMap<u32, u32> = HashMap::new();
    expected_neighborhood.insert(node_2, 1);
    expected_neighborhood.insert(node_4, 2);
    assert_eq!(neighborhood, expected_neighborhood);

    // Adding 4 to the clique, so 4 is no longer adjacent and 3 should
    // be added with value 1.
    candidate.add_node(node_4)?;
    let neighborhood = candidate.get_neighborhood();
    let mut expected_neighborhood: HashMap<u32, u32> = HashMap::new();
    expected_neighborhood.insert(node_2, 1);
    expected_neighborhood.insert(node_3, 1);
    assert_eq!(neighborhood, expected_neighborhood);

    // Adding 3 to the clique, so 3 is no longer adjacent and 6 should
    // be added with value 1.
    candidate.add_node(node_3)?;
    let neighborhood = candidate.get_neighborhood();
    let mut expected_neighborhood: HashMap<u32, u32> = HashMap::new();
    expected_neighborhood.insert(node_2, 1);
    expected_neighborhood.insert(node_6, 1);
    assert_eq!(neighborhood, expected_neighborhood);

    Ok(())
}

/// Tests incremental versions of the candidate functioins.
/// (Does not inspect the density guarantee itself.)
///
///  1 - 2
///    \\
///  3 - 4
///    \
///  5 - 6
///
/// Start with {1, 4}, then add 3, then add 6.
#[test]
fn test_incremental() -> CLQResult<()> {
    let (graph, transformer) = build_sample_graph();
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id: u32 = 1;
    let scorer: Scorer = Scorer::new(2, &transformer.search_problem);

    let node_3 = graph.get_node_by_label(3.into());
    let node_4 = graph.get_node_by_label(4.into());
    let node_6 = graph.get_node_by_label(6.into());

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    // Adding 4 to the clique, so both of the possible edges should exist.
    let new_size = candidate.get_size_with_node(&node_4)?;
    let new_cliqueness = candidate.get_cliqueness_with_node(&node_4)?;
    assert!(candidate.local_thresh_score_with_node_at_least(1.0, &node_4).0);
    candidate.add_node(node_4.node_id)?;
    assert_eq!(new_size, candidate.get_size()?);
    assert_eq!(new_cliqueness, candidate.get_cliqueness()?);

    // Adding 3 to the clique. Expected local densities: {1: 1.0, 3: 0.5}
    let new_size = candidate.get_size_with_node(&node_3)?;
    let new_cliqueness = candidate.get_cliqueness_with_node(&node_3)?;
    assert!(candidate.local_thresh_score_with_node_at_least(0.5, &node_3).0);
    assert!(!candidate.local_thresh_score_with_node_at_least(0.51, &node_3).0);
    candidate.add_node(node_3.node_id)?;
    assert_eq!(new_size, candidate.get_size()?);
    assert_eq!(new_cliqueness, candidate.get_cliqueness()?);

    // Adding 6 to the clique. Expected local densities: {1: 0.5, 3: 0.5}
    let new_size = candidate.get_size_with_node(&node_6)?;
    let new_cliqueness = candidate.get_cliqueness_with_node(&node_6)?;
    candidate.add_node(node_6.node_id)?;
    assert_eq!(new_size, candidate.get_size()?);
    assert_eq!(new_cliqueness, candidate.get_cliqueness()?);
    Ok(())
}

/// Test that a candidate's appropriately calculates its local density.
/// (Does not inspect the density guarantee itself.)
///
///  1 - 2
///    \\
///  3 - 4
///    \
///  5 - 6
///
/// Start with {1, 4}, then add 3, then add 6.
#[test]
fn test_local_density() -> CLQResult<()> {
    let (graph, transformer) = build_sample_graph();
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id: u32 = 1;
    let scorer: Scorer = Scorer::new(2, &transformer.search_problem);

    let node_3: u32 = graph.get_node_by_label(3.into()).node_id;
    let node_4: u32 = graph.get_node_by_label(4.into()).node_id;
    let node_6: u32 = graph.get_node_by_label(6.into()).node_id;

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    // Adding 4 to the clique, so both of the possible edges should exist.
    // Local density is 1.0 for node 1.
    candidate.add_node(node_4)?;
    assert!(candidate.local_thresh_score_at_least(1.0));

    // Adding 3 to the clique. Expected local densities: {1: 1.0, 3: 0.5}
    candidate.add_node(node_3)?;
    assert!(candidate.local_thresh_score_at_least(0.5));
    assert!(!candidate.local_thresh_score_at_least(0.51));

    // Adding 6 to the clique. Expected local densities: {1: 0.5, 3: 0.5}
    candidate.add_node(node_6)?;
    assert!(candidate.local_thresh_score_at_least(0.5));
    assert!(!candidate.local_thresh_score_at_least(0.51));

    // Try the same scenario, but without checking any intermediate values
    // (to allow exceptions list to build).
    let mut candidate2: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    // Adding 4 to the clique, so both of the possible edges should exist.
    // Local density is 1.0 for node 1.
    candidate2.add_node(node_4)?;
    // Adding 3 to the clique. Expected local densities: {1: 1.0, 3: 0.5}
    candidate2.add_node(node_3)?;
    // Adding 6 to the clique. Expected local densities: {1: 0.5, 3: 0.5}
    candidate2.add_node(node_6)?;
    assert!(candidate2.local_thresh_score_at_least(0.5));
    assert!(!candidate2.local_thresh_score_at_least(0.51));
    Ok(())
}

/// Test that a candidate is appropriately calculating / updating its local density guarantee.
/// (Note that this test is a bit of an abstraction violation: Candidate doesn't technically
/// specify any contract about how it will choose to update its local guarantee, only that
/// whatever density guarantee it makes will be met.
/// We have a test here anyway, because this is among the trickier bits of code.)
///
///  1 - 2
///    \\
///  3 - 4
///    \
///  5 - 6
///
/// Start with {1, 4}, then add 2, then add 3.
#[test]
fn test_local_density_guarantees() -> CLQResult<()> {
    let (graph, transformer) = build_sample_graph();
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id: u32 = 1;
    let scorer: Scorer = Scorer::new(2, &transformer.search_problem);

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    let node_2: u32 = graph.get_node_by_label(2.into()).node_id;
    let node_4: u32 = graph.get_node_by_label(4.into()).node_id;

    // Adding 4 to the clique, so both of the possible edges should exist.
    candidate.add_node(node_4)?;
    assert!(candidate.local_thresh_score_at_least(1.0));
    // Since we've checked the local_thresh score and got a true value,
    // we should know the exact values: at least 2 edges per node, no exceptions.
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 2);
    assert!(guarantee.exceptions.is_empty());

    // Adding 2 to the clique. Expected local density: {1: .75}.
    // Right shore node added, so guarantee should be unchanged.

    candidate.add_node(node_2)?;
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 2);
    assert!(guarantee.exceptions.is_empty());

    // Note: This doesn't work yet.
    // After checking that we have at least .75 density, guarantee
    // should be updated to say we have at least 3 edges.
    assert!(candidate.local_thresh_score_at_least(0.75));
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 3);
    assert!(guarantee.exceptions.is_empty());

    // Adding 3 to the clique. Expected local densities: {1: 0.75, 3: 0.25}
    // Before we check the guarantee, we should have it as an exception.

    let new_core_node: u32 = graph.get_node_by_label(3.into()).node_id;
    candidate.add_node(new_core_node)?;
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 3);
    assert!(guarantee.exceptions.contains(new_core_node));
    assert_eq!(guarantee.exceptions.len(), 1);
    // A failed local density check shouldn't give us any new info.
    assert!(!candidate.local_thresh_score_at_least(0.75));
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 3);
    assert!(guarantee.exceptions.contains(new_core_node));
    assert_eq!(guarantee.exceptions.len(), 1);
    // A passing local density check should give us a new guarantee with
    // no exceptions.
    assert!(candidate.local_thresh_score_at_least(0.22));
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 1);
    assert!(guarantee.exceptions.is_empty());

    Ok(())
}

/// Test that a candidate property performs a one-step search.
///
///  1 - 2
///    \
///  3 - 4
///    \
///  5 - 6
///
/// Start with {1, 3, 5}. Expansion candidates should rank 1,2,4.
#[test]
fn test_one_step_search() -> CLQResult<()> {
    let (graph, transformer) = build_sample_graph();
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id: u32 = 1;
    let scorer: Scorer = Scorer::new(2, &transformer.search_problem);

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    let node_3: u32 = graph.get_node_by_label(3.into()).node_id;
    let node_5: u32 = graph.get_node_by_label(5.into()).node_id;

    // Adding 3 and 5 to the clique.
    candidate.add_node(node_3)?;
    candidate.add_node(node_5)?;

    let mut visited_candidates: HashSet<u64> = HashSet::new();
    let recipes: Vec<Recipe> = candidate
        .one_step_search(2, &mut visited_candidates, &scorer)
        .unwrap();

    // When we do a one step search, it should respect the num_to_search arugument...
    assert_eq!(recipes.len(), 2);
    // ... and we should only get recipes that involve adding 4 and 6 added, not 2.
    // (because of the num_ties with the original 3 nodes.)
    let node_2: u32 = graph.get_node_by_label(2.into()).node_id;
    let node_4: u32 = graph.get_node_by_label(4.into()).node_id;
    let node_6: u32 = graph.get_node_by_label(6.into()).node_id;

    for recipe in recipes {
        assert!(recipe.checksum == candidate.checksum);
        assert!(recipe.node_id != Some(node_2));
        assert!(recipe.node_id == Some(node_4) || recipe.node_id == Some(node_6));
    }

    Ok(())
}
