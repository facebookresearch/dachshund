/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;

use std::rc::Rc;

use lib_dachshund::dachshund::candidate::Candidate;
use lib_dachshund::dachshund::error::CLQResult;
use lib_dachshund::dachshund::id_types::{GraphId, NodeId};
use lib_dachshund::dachshund::row::EdgeRow;
use lib_dachshund::dachshund::scorer::Scorer;
use lib_dachshund::dachshund::search_problem::SearchProblem;
use lib_dachshund::dachshund::transformer::Transformer;
use lib_dachshund::dachshund::typed_graph::TypedGraph;

use lib_dachshund::dachshund::test_utils::{gen_test_transformer, process_raw_vector};

#[cfg(test)]
#[test]
fn test_score_trivial_graph() -> CLQResult<()> {
    let typespec: Vec<Vec<String>> = vec![
        vec![
            "author".to_string(),
            "published_at".into(),
            "conference".into(),
        ],
        vec!["author".to_string(), "attended".into(), "conference".into()],
    ];
    let graph_id: GraphId = 0.into();
    let raw: Vec<String> = vec!["0\t1\t2\tauthor\tpublished_at\tconference".to_string()];
    let transformer: Transformer = gen_test_transformer(typespec, "author".to_string())?;
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw)?;
    let graph: TypedGraph = transformer.build_pruned_graph(graph_id, rows)?;
    assert_eq!(graph.core_ids.len(), 1);
    assert_eq!(graph.non_core_ids.len(), 1);

    let alpha = 1.0;
    let search_problem = Rc::new(SearchProblem::new(
        20,
        alpha,
        Some(0.5),
        Some(0.5),
        20,
        100,
        3,
        1,
    ));

    let scorer: Scorer = Scorer::new(2, &search_problem);
    let core_node_id: NodeId = *graph.core_ids.first().unwrap();
    let mut candidate: Candidate<TypedGraph> = Candidate::new(core_node_id, &graph, &scorer)?;
    assert_eq!(candidate.get_score()?, -1.0);

    let non_core_node_id: NodeId = *graph.non_core_ids.first().unwrap();
    candidate.add_node(non_core_node_id)?;
    assert!(
        candidate.get_score().is_err(),
        "Candidate should have score is None."
    );

    let non_core_diversity_score: f32 = scorer.get_non_core_diversity_score(&candidate)?;
    let expected_non_core_diversity_score: f32 = (2.0 as f32).ln();
    assert_eq!(non_core_diversity_score, expected_non_core_diversity_score);

    let local_threshold_score: f32 = scorer.get_local_thresh_score(&mut candidate);
    let expected_local_threshold_score: f32 = 1.0 as f32;
    assert_eq!(local_threshold_score, expected_local_threshold_score);

    // size is 2 since each author could be connected to two types of non_cores
    let size: usize = candidate.get_size()?;
    assert_eq!(size, 2);

    let ties_between_nodes: usize = candidate.count_ties_between_nodes()?;
    assert_eq!(ties_between_nodes, 1);

    // cliqueness should be 0.5 because only half the connections are present.
    let cliqueness: f32 = candidate.get_cliqueness()?;
    assert_eq!(cliqueness, 0.5 as f32);

    let global_threshold_score: f32 = scorer.get_global_thresh_score(cliqueness);
    assert_eq!(global_threshold_score, 1.0 as f32);

    let score: f32 = scorer.score(&mut candidate)?;
    let expected_score: f32 = (1.0 as f32 + graph.core_ids.len() as f32).ln()
        + (non_core_diversity_score + cliqueness * alpha);
    assert_eq!(score, expected_score);
    Ok(())
}
