/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;

use rand::seq::SliceRandom;
use rand::thread_rng;

use lib_dachshund::dachshund::beam::Beam;
use lib_dachshund::dachshund::candidate::Candidate;
use lib_dachshund::dachshund::error::{CLQError, CLQResult};
use lib_dachshund::dachshund::id_types::{GraphId, NodeId, NodeTypeId};
use lib_dachshund::dachshund::input::Input;
use lib_dachshund::dachshund::output::Output;
use lib_dachshund::dachshund::row::CliqueRow;
use lib_dachshund::dachshund::row::EdgeRow;
use lib_dachshund::dachshund::test_utils::{
    assert_nodes_have_ids, gen_test_transformer, process_raw_vector,
};
use lib_dachshund::dachshund::transformer::Transformer;
use lib_dachshund::dachshund::transformer_base::TransformerBase;
use lib_dachshund::dachshund::typed_graph::TypedGraph;
use lib_dachshund::dachshund::typed_graph_builder::TypedGraphBuilder;

#[cfg(test)]
#[test]
fn test_init_beam_with_clique_rows() -> CLQResult<()> {
    let typespec: Vec<Vec<String>> = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let target_types: Vec<String> = vec!["article".to_string()];
    let raw = vec![
        "0\t1\t3\tauthor\tpublished\tarticle".to_string(),
        "0\t2\t3\tauthor\tpublished\tarticle".into(),
        "0\t1\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t5\tauthor\tpublished\tarticle".into(),
    ];
    let graph_id: GraphId = 0.into();
    let transformer: Transformer = gen_test_transformer(typespec, "author".to_string())?;
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw)?;
    let target_type_ids = &transformer.non_core_type_ids;
    let article_type: NodeTypeId = *target_type_ids.require("article")?;
    assert_eq!(article_type.value(), 1);
    let clique_rows: Vec<CliqueRow> = vec![
        CliqueRow::new(graph_id, 1, None),
        CliqueRow::new(graph_id, 3, Some(article_type)),
        CliqueRow::new(graph_id, 4, Some(article_type)),
    ];
    let graph: TypedGraph =
        transformer.build_pruned_graph::<TypedGraphBuilder, TypedGraph>(graph_id, &rows)?;
    let test_node_id: NodeId = NodeId::from(3 as i64);
    graph.nodes[&test_node_id]
        .non_core_type
        .ok_or_else(CLQError::err_none)?;

    let beam: Beam<TypedGraph> = Beam::new(
        &graph,
        &clique_rows,
        20,
        false,
        &target_types,
        1,
        1.0,
        Some(1.0),
        Some(1.0),
        graph_id,
    )?;
    let init_candidate: &Candidate<TypedGraph> = &beam.candidates[0];
    assert_nodes_have_ids(&graph, &init_candidate.core_ids, vec![1], true);
    assert_nodes_have_ids(&graph, &init_candidate.non_core_ids, vec![3, 4], false);
    Ok(())
}

#[test]
fn test_init_beam_with_partially_overlapping_clique_rows() -> CLQResult<()> {
    let typespec: Vec<Vec<String>> = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let target_types: Vec<String> = vec!["article".to_string()];
    let raw = vec![
        "0\t1\t3\tauthor\tpublished\tarticle".to_string(),
        "0\t2\t3\tauthor\tpublished\tarticle".into(),
        "0\t1\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t5\tauthor\tpublished\tarticle".into(),
    ];
    let graph_id: GraphId = 0.into();

    let transformer: Transformer = gen_test_transformer(typespec, "author".to_string())?;
    let rows: Vec<EdgeRow> = process_raw_vector(&transformer, raw)?;
    let target_type_ids = &transformer.non_core_type_ids;
    let article_type: NodeTypeId = *target_type_ids.require("article")?;
    let clique_rows: Vec<CliqueRow> = vec![
        CliqueRow::new(graph_id, 1, None),
        CliqueRow::new(graph_id, 8, None),
        CliqueRow::new(graph_id, 3, Some(article_type)),
        CliqueRow::new(graph_id, 4, Some(article_type)),
        CliqueRow::new(graph_id, 7, Some(article_type)),
    ];
    let graph: TypedGraph =
        transformer.build_pruned_graph::<TypedGraphBuilder, TypedGraph>(graph_id, &rows)?;
    let beam: Beam<TypedGraph> = Beam::new(
        &graph,
        &clique_rows,
        20,
        false,
        &target_types,
        1,
        1.0,
        Some(1.0),
        Some(1.0),
        graph_id,
    )?;
    let init_candidate: &Candidate<TypedGraph> = &beam.candidates[0];
    assert_nodes_have_ids(&graph, &init_candidate.core_ids, vec![1], true);
    assert_nodes_have_ids(&graph, &init_candidate.non_core_ids, vec![3, 4], false);
    Ok(())
}

#[test]
fn test_init_beam_with_clique_rows_input() -> CLQResult<()> {
    let do_test = |should_jumble_rows| -> CLQResult<()> {
        let typespec: Vec<Vec<String>> = vec![
            vec!["author".to_string(), "published".into(), "article".into()],
            vec!["author".to_string(), "cited".into(), "article".into()],
        ];
        let mut raw = vec![
            "0\t1\t3\tauthor\tpublished\tarticle".to_string(),
            "0\t2\t3\tauthor\tpublished\tarticle".into(),
            "0\t1\t4\tauthor\tpublished\tarticle".into(),
            "0\t2\t4\tauthor\tpublished\tarticle".into(),
            "0\t2\t5\tauthor\tpublished\tarticle".into(),
            "0\t1\tauthor\t\t\t".into(),
            "0\t3\tarticle\t\t\t".into(),
            "0\t4\tarticle\t\t\t".into(),
        ];
        if should_jumble_rows {
            raw.shuffle(&mut thread_rng());
        }
        let expected = vec![
            "0\t1\tauthor".to_string(),
            "0\t3\tarticle".into(),
            "0\t4\tarticle".into(),
        ];
        // transformer with no epochs
        let mut transformer = Transformer::new(
            typespec,
            20,
            1.0,
            Some(0.5),
            Some(0.5),
            20,
            0,
            3,
            true,
            0,
            "author".to_string(),
            true,
        )?;
        let text = raw.join("\n");
        let bytes = text.as_bytes();
        let input = Input::string(&bytes);
        let mut buffer: Vec<u8> = Vec::new();
        let output = Output::string(&mut buffer);
        transformer.run(input, output)?;
        let output_str: String = String::from_utf8(buffer)?;
        assert_eq!(output_str, expected.join("\n") + "\n");
        Ok(())
    };
    do_test(false)?;
    do_test(true)?;
    Ok(())
}

#[test]
fn test_init_beam_with_clique_rows_input_one_epoch() -> CLQResult<()> {
    let typespec: Vec<Vec<String>> = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let raw = vec![
        "0\t1\t3\tauthor\tpublished\tarticle".to_string(),
        "0\t2\t3\tauthor\tpublished\tarticle".into(),
        "0\t1\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t5\tauthor\tpublished\tarticle".into(),
        "0\t1\tauthor\t\t\t".into(),
        "0\t3\tarticle\t\t\t".into(),
        "0\t4\tarticle\t\t\t".into(),
    ];
    let expected = vec![
        "0\t1\tauthor".to_string(),
        "0\t2\tauthor".into(),
        "0\t3\tarticle".into(),
        "0\t4\tarticle".into(),
    ];
    // transformer with no epochs
    let mut transformer = Transformer::new(
        typespec,
        20,
        1.0,
        Some(0.5),
        Some(0.5),
        20,
        // one epoch rather than 0
        1,
        3,
        true,
        0,
        "author".to_string(),
        true,
    )?;
    let text = raw.join("\n");
    let bytes = text.as_bytes();
    let input = Input::string(&bytes);
    let mut buffer: Vec<u8> = Vec::new();
    let output = Output::string(&mut buffer);
    transformer.run(input, output)?;
    let output_str: String = String::from_utf8(buffer)?;
    assert_eq!(output_str, expected.join("\n") + "\n");
    Ok(())
}

#[test]
fn test_beam_with_empty_graph_after_pruning() -> CLQResult<()> {
    let typespec: Vec<Vec<String>> = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let raw = vec![
        "0\t1\t3\tauthor\tpublished\tarticle".to_string(),
        "0\t2\t3\tauthor\tpublished\tarticle".into(),
        "0\t1\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t4\tauthor\tpublished\tarticle".into(),
        "0\t2\t5\tauthor\tpublished\tarticle".into(),
        "0\t1\tauthor\t\t\t".into(),
        "0\t3\tarticle\t\t\t".into(),
        "0\t4\tarticle\t\t\t".into(),
    ];
    // transformer with no epochs
    let mut transformer = Transformer::new(
        typespec,
        20,
        1.0,
        Some(0.5),
        Some(0.5),
        20,
        100,
        3,
        true,
        // min degree of 10, should remove all nodes
        10,
        "author".to_string(),
        true,
    )?;
    let text = raw.join("\n");
    let bytes = text.as_bytes();
    let input = Input::string(&bytes);
    let mut buffer: Vec<u8> = Vec::new();
    let output = Output::string(&mut buffer);
    transformer.run(input, output)?;
    let output_str: String = String::from_utf8(buffer)?;
    assert_eq!(output_str, "");
    Ok(())
}
