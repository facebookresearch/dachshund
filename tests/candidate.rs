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
    let node: Node = Node::new(node_id, true, None, Vec::new());
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
    let score: f32 = scorer.score(&candidate)?;
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
