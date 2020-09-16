/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;

use std::collections::HashMap;

use lib_dachshund::dachshund::candidate::Candidate;
use lib_dachshund::dachshund::error::CLQResult;
use lib_dachshund::dachshund::id_types::{GraphId, NodeId};
use lib_dachshund::dachshund::node::Node;
use lib_dachshund::dachshund::row::CliqueRow;
use lib_dachshund::dachshund::row::EdgeRow;
use lib_dachshund::dachshund::scorer::Scorer;
use lib_dachshund::dachshund::test_utils::{gen_test_transformer, process_raw_vector};
use lib_dachshund::dachshund::transformer::Transformer;
use lib_dachshund::dachshund::typed_graph::TypedGraph;
use lib_dachshund::dachshund::typed_graph_builder::TypedGraphBuilder;

#[cfg(test)]
#[test]
fn test_output_simple_candidate() -> CLQResult<()> {
    let node_id = NodeId::from(0);
    let node: Node = Node::new(node_id, true, None, Vec::new(), HashMap::new());
    let mut graph: TypedGraph = TypedGraph {
        nodes: HashMap::new(),
        core_ids: vec![],
        non_core_ids: vec![],
    };
    graph.nodes.insert(node_id, node);
    graph.core_ids.push(node_id);

    let mut candidate: Candidate<TypedGraph> = Candidate::init_blank(&graph);
    candidate.add_node(node_id)?;
    let graph_id: GraphId = 1.into();
    let output_rows: Vec<CliqueRow> = candidate.get_output_rows(graph_id)?;
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
    let graph: TypedGraph =
        transformer.build_pruned_graph::<TypedGraphBuilder, TypedGraph>(graph_id, &rows)?;
    assert_eq!(graph.core_ids.len(), 1);
    let core_node_id: NodeId = *graph.core_ids.first().unwrap();
    assert_eq!(graph.non_core_ids.len(), 1);
    let non_core_node_id: NodeId = *graph.non_core_ids.first().unwrap();

    let alpha: f32 = 1.0;
    let scorer: Scorer = Scorer::new(2, alpha, Some(0.5), Some(0.5));
    let mut candidate: Candidate<TypedGraph> = Candidate::new(core_node_id, &graph, &scorer)?;
    candidate.add_node(non_core_node_id)?;
    let score: f32 = scorer.score(&mut candidate)?;
    candidate.set_score(score)?;

    let graph_id: GraphId = 1.into();
    let output_rows: Vec<CliqueRow> = candidate.get_output_rows(graph_id)?;
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
fn build_sample_graph() -> CLQResult<TypedGraph>{
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

    let transformer: Transformer = gen_test_transformer(typespec, "author".to_string())?;
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw)?;
    transformer.build_pruned_graph::<TypedGraphBuilder, TypedGraph>(graph_id, &rows)
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
    let graph: TypedGraph = build_sample_graph()?;
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id: NodeId = 1.into();
    let alpha: f32 = 1.0;
    let scorer: Scorer = Scorer::new(2, alpha, Some(0.0), Some(0.0));

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    let neighborhood = candidate.get_neighborhood();
    let mut expected_neighborhood: HashMap<NodeId, usize> = HashMap::new();
    expected_neighborhood.insert(2.into(), 1);
    expected_neighborhood.insert(4.into(), 2);
    assert_eq!(neighborhood, expected_neighborhood);

    // Adding 4 to the clique, so 4 is no longer adjacent and 3 should
    // be added with value 1.
    candidate.add_node(4.into())?;
    let neighborhood = candidate.get_neighborhood();
    let mut expected_neighborhood: HashMap<NodeId, usize> = HashMap::new();
    expected_neighborhood.insert(2.into(), 1);
    expected_neighborhood.insert(3.into(), 1);
    assert_eq!(neighborhood, expected_neighborhood);

    // Adding 3 to the clique, so 3 is no longer adjacent and 6 should
    // be added with value 1.
    candidate.add_node(3.into())?;
    let neighborhood = candidate.get_neighborhood();
    let mut expected_neighborhood: HashMap<NodeId, usize> = HashMap::new();
    expected_neighborhood.insert(2.into(), 1);
    expected_neighborhood.insert(6.into(), 1);
    assert_eq!(neighborhood, expected_neighborhood);

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
    let graph: TypedGraph = build_sample_graph()?;
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id : NodeId = 1.into();
    let alpha: f32 = 1.0;
    let scorer: Scorer = Scorer::new(2, alpha, Some(0.0), Some(0.0));

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    // Adding 4 to the clique, so both of the possible edges should exist.
    // Local density is 1.0 for node 1.
    candidate.add_node(4.into())?;
    assert!(candidate.local_thresh_score_at_least(1.0));

    // Adding 3 to the clique. Expected local densities: {1: 1.0, 3: 0.5}
    candidate.add_node(3.into())?;
    assert!(candidate.local_thresh_score_at_least(0.5));
    assert!(!candidate.local_thresh_score_at_least(0.51));

    // Adding 6 to the clique. Expected local densities: {1: 0.5, 3: 0.5}
    candidate.add_node(3.into())?;
    assert!(candidate.local_thresh_score_at_least(0.5));
    assert!(!candidate.local_thresh_score_at_least(0.51));

    // Try the same scenario, but without checking any intermediate values
    // (to allow exceptions list to build).
    let mut candidate2: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    // Adding 4 to the clique, so both of the possible edges should exist.
    // Local density is 1.0 for node 1.
    candidate2.add_node(4.into())?;
    // Adding 3 to the clique. Expected local densities: {1: 1.0, 3: 0.5}
    candidate2.add_node(3.into())?;
    // Adding 6 to the clique. Expected local densities: {1: 0.5, 3: 0.5}
    candidate2.add_node(3.into())?;
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
    let graph: TypedGraph = build_sample_graph()?;
    assert_eq!(graph.core_ids.len(), 3);
    assert_eq!(graph.non_core_ids.len(), 3);

    let initial_id : NodeId = 1.into();
    let alpha: f32 = 1.0;
    let scorer: Scorer = Scorer::new(2, alpha, Some(0.0), Some(0.0));

    let mut candidate: Candidate<TypedGraph> = Candidate::new(initial_id, &graph, &scorer)?;

    // Adding 4 to the clique, so both of the possible edges should exist.
    candidate.add_node(4.into())?;
    assert!(candidate.local_thresh_score_at_least(1.0));
    // Since we've checked the local_thresh score and got a true value,
    // we should know the exact values: at least 2 edges per node, no exceptions.
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 2);
    assert!(guarantee.exceptions.is_empty());

    // Adding 2 to the clique. Expected local density: {1: .75}.
    // Right shore node added, so guarantee should be unchanged.
    candidate.add_node(2.into())?;
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
    let new_core_node : NodeId = 3.into();
    candidate.add_node(new_core_node)?;
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 3);
    assert!(guarantee.exceptions.contains(&new_core_node));
    assert_eq!(guarantee.exceptions.len(), 1);
    // A failed local density check shouldn't give us any new info.
    assert!(!candidate.local_thresh_score_at_least(0.75));
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 3);
    assert!(guarantee.exceptions.contains(&new_core_node));
    assert_eq!(guarantee.exceptions.len(), 1);
    // A passing local density check should give us a new guarantee with
    // no exceptions.
    assert!(candidate.local_thresh_score_at_least(0.22));
    let guarantee = candidate.get_local_guarantee();
    assert_eq!(guarantee.num_edges, 1);
    assert!(guarantee.exceptions.is_empty());

    Ok(())
}
