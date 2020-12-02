/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
extern crate lib_dachshund;

use lib_dachshund::dachshund::candidate::Candidate;
use lib_dachshund::dachshund::error::{CLQError, CLQResult};
use lib_dachshund::dachshund::id_types::{GraphId, NodeId};
use lib_dachshund::dachshund::line_processor::LineProcessorBase;
use lib_dachshund::dachshund::row::{CliqueRow, EdgeRow};
use lib_dachshund::dachshund::test_utils::{
    assert_nodes_have_ids, gen_single_clique, gen_test_transformer, gen_test_typespec,
    process_raw_vector,
};
use lib_dachshund::dachshund::transformer::Transformer;
use lib_dachshund::dachshund::typed_graph::TypedGraph;
use std::sync::mpsc::channel;

#[cfg(test)]
#[test]
fn test_process_typespec() -> CLQResult<()> {
    let ts = vec![
        vec![
            "author".to_string(),
            "published_at".into(),
            "conference".into(),
        ],
        vec![
            "author".to_string(),
            "organized".into(),
            "conference".into(),
        ],
        vec![
            "author".to_string(),
            "published_at".into(),
            "journal".into(),
        ],
        vec!["author".to_string(), "attended".into(), "conference".into()],
    ];
    let target_types = vec!["conference".to_string(), "journal".into()];
    let core_type: String = "author".to_string();
    let target_type_ids = Transformer::process_typespec(ts, &core_type, target_types)?;
    assert_eq!(target_type_ids.require("conference")?.value(), 1);
    assert_eq!(target_type_ids.require("journal")?.value(), 2);
    assert_eq!(
        target_type_ids
            .require("conference")?
            .max_edge_count_with_core_node()
            .unwrap(),
        3
    );
    assert_eq!(
        target_type_ids
            .require("journal")?
            .max_edge_count_with_core_node()
            .unwrap(),
        1
    );
    Ok(())
}

#[test]
fn test_process_single_line() -> CLQResult<()> {
    let ts = gen_test_typespec();
    let transformer = gen_test_transformer(ts, "author".to_string())?;
    // graph_id source_id target_id target_type
    let raw: String = "0\t1\t2\tauthor\tpublished_at\tjournal".to_string();

    let row: EdgeRow = transformer
        .line_processor
        .process_line(raw)?
        .as_edge_row()
        .ok_or_else(CLQError::err_none)?;
    assert_eq!(row.graph_id.value(), 0);
    assert_eq!(row.source_id, NodeId::from(1));
    assert_eq!(row.target_id, NodeId::from(2));
    let target_type_name: Option<String> =
        transformer.non_core_type_ids.type_name(&row.target_type_id);
    assert_eq!(target_type_name, Some("journal".to_owned()));
    Ok(())
}

#[test]
fn test_process_single_line_clique_row() -> CLQResult<()> {
    let ts = gen_test_typespec();
    let transformer = gen_test_transformer(ts, "author".to_string())?;
    // graph_id node_id node_type
    let raw: String = "0\t2\tjournal\t\t\t".to_string();
    let row: CliqueRow = transformer
        .line_processor
        .process_line(raw)?
        .as_clique_row()
        .unwrap();
    assert_eq!(row.graph_id.value(), 0);
    assert_eq!(row.node_id, NodeId::from(2));
    let target_type_name: Option<String> = transformer
        .non_core_type_ids
        .type_name(&row.target_type.unwrap());
    assert_eq!(target_type_name, Some("journal".to_owned()));
    let raw: String = "0\t1\tauthor\t\t\t".to_string();
    let row: CliqueRow = transformer
        .line_processor
        .process_line(raw)?
        .as_clique_row()
        .unwrap();
    assert_eq!(row.graph_id.value(), 0);
    assert_eq!(row.node_id, NodeId::from(1));
    assert_eq!(row.target_type, None);
    Ok(())
}

fn test_expected_clique<F>(transformer: Transformer, raw: Vec<String>, f: F) -> CLQResult<()>
where
    F: Fn(&TypedGraph, &Candidate<TypedGraph>) -> (),
{
    let graph_id: GraphId = 0.into();

    let rows = process_raw_vector(&transformer, raw).unwrap();
    let graph: TypedGraph = transformer
        .build_pruned_graph(graph_id, &rows)
        .unwrap();
    let clique_rows = Vec::new();
    let (sender, _receiver) = channel();
    let res: Candidate<TypedGraph> = transformer
        .process_clique_rows(
            &graph,
            &clique_rows,
            graph_id,
            true,
            &sender,
        )
        .unwrap()
        .ok_or_else(CLQError::err_none)
        .unwrap()
        .top_candidate;
    sender.send((None, true)).unwrap();
    f(&graph, &res);
    Ok(())
}

#[test]
fn test_process_single_row() -> CLQResult<()> {
    test_expected_clique(
        gen_test_transformer(gen_test_typespec(), "author".to_string()).unwrap(),
        vec!["0\t1\t2\tauthor\tpublished_at\tconference".to_string()],
        |graph, res| {
            assert_nodes_have_ids(graph, &res.core_ids, vec![1], true);
            assert_nodes_have_ids(graph, &res.non_core_ids, vec![2], false);
        },
    )
}

#[test]
fn test_process_small_clique() -> CLQResult<()> {
    test_expected_clique(
        gen_test_transformer(gen_test_typespec(), "author".to_string()).unwrap(),
        vec![
            "0\t1\t3\tauthor\tpublished_at\tconference".to_string(),
            "0\t2\t3\tauthor\tpublished_at\tconference".into(),
            "0\t1\t4\tauthor\tpublished_at\tconference".into(),
            "0\t2\t4\tauthor\tpublished_at\tconference".into(),
        ],
        |graph, res| {
            assert_nodes_have_ids(graph, &res.core_ids, vec![1, 2], true);
            assert_nodes_have_ids(graph, &res.non_core_ids, vec![3, 4], false);
        },
    )
}

#[test]
fn test_process_small_clique_with_non_clique_row() -> CLQResult<()> {
    test_expected_clique(
        gen_test_transformer(gen_test_typespec(), "author".to_string()).unwrap(),
        vec![
            "0\t1\t3\tauthor\tpublished_at\tconference".to_string(),
            "0\t2\t3\tauthor\tpublished_at\tconference".into(),
            "0\t1\t4\tauthor\tpublished_at\tconference".into(),
            "0\t2\t4\tauthor\tpublished_at\tconference".into(),
            // nonsensical
            "0\t2\t5\tconference\tpublished_at\tconference".into(),
        ],
        |graph, res| {
            assert_nodes_have_ids(graph, &res.core_ids, vec![1, 2], true);
            assert_nodes_have_ids(graph, &res.non_core_ids, vec![3, 4], false);
        },
    )
}

#[test]
fn test_process_medium_clique() -> CLQResult<()> {
    let ts = gen_test_typespec();
    let non_core_types = ts.iter().map(|x| x[2].clone()).collect();
    let graph_id: GraphId = 0.into();
    let (core_ids, non_cores, clique_rows) = gen_single_clique(
        graph_id,
        10,
        vec![10, 10],
        non_core_types,
        "author".to_string(),
        vec!["published_at".to_string()],
    );
    assert_eq!(clique_rows.len(), 200);
    test_expected_clique(
        gen_test_transformer(ts, "author".to_string()).unwrap(),
        clique_rows,
        |graph, res| {
            assert_nodes_have_ids(
                graph,
                &res.core_ids,
                core_ids.iter().map(|x| x.value()).collect(),
                true,
            );
            assert_nodes_have_ids(
                graph,
                &res.non_core_ids,
                non_cores.iter().map(|x| x.0.value()).collect(),
                false,
            );
        },
    )
}

#[test]
fn test_process_medium_clique_with_insufficient_epochs() -> CLQResult<()> {
    let ts = gen_test_typespec();
    let non_core_types = ts.iter().map(|x| x[2].clone()).collect();
    let graph_id: GraphId = 0.into();
    let (_core_ids, _non_cores, clique_rows) = gen_single_clique(
        graph_id,
        10,
        vec![10, 10],
        non_core_types,
        "author".to_string(),
        vec!["published_at".to_string()],
    );
    assert_eq!(clique_rows.len(), 200);
    test_expected_clique(
        Transformer::new(
            ts,
            20,
            1.0,
            Some(1.0),
            Some(1.0),
            20,
            10,
            3,
            true, // with 10 epochs
            0,    // min_degree = 0
            "author".to_string(),
            false,
        )?,
        clique_rows,
        |_graph, res| {
            assert_eq!(res.core_ids.len() + res.non_core_ids.len(), 11);
        },
    )
}

#[test]
fn test_process_small_clique_with_two_kinds_of_rows() -> CLQResult<()> {
    let typespec = vec![
        vec![
            "author".to_string(),
            "published_at".into(),
            "conference".into(),
        ],
        vec!["author".to_string(), "attended".into(), "conference".into()],
    ];
    let raw = vec![
        "0\t1\t3\tauthor\tpublished_at\tconference".to_string(),
        "0\t2\t3\tauthor\tpublished_at\tconference".into(),
        "0\t1\t3\tauthor\tattended\tconference".into(),
        "0\t2\t3\tauthor\tattended\tconference".into(),
    ];
    test_expected_clique(
        gen_test_transformer(typespec, "author".to_string()).unwrap(),
        raw,
        |graph, res| {
            assert_nodes_have_ids(graph, &res.core_ids, vec![1, 2], true);
            assert_nodes_have_ids(graph, &res.non_core_ids, vec![3], false);
        },
    )
}

#[test]
fn test_process_another_small_clique_with_two_kinds_of_rows() -> CLQResult<()> {
    let typespec = vec![
        vec!["author".to_string(), "published".into(), "article".into()],
        vec!["author".to_string(), "cited".into(), "article".into()],
    ];
    let raw = vec![
        "0\t1\t5\tauthor\tpublished\tarticle".to_string(),
        "0\t0\t5\tauthor\tpublished\tarticle".into(),
        "0\t2\t5\tauthor\tpublished\tarticle".into(),
        "0\t3\t5\tauthor\tpublished\tarticle".into(),
        "0\t2\t5\tauthor\tcited\tarticle".into(),
        "0\t4\t5\tauthor\tpublished\tarticle".into(),
        "0\t3\t5\tauthor\tcited\tarticle".into(),
    ];
    test_expected_clique(
        gen_test_transformer(typespec, "author".to_string()).unwrap(),
        raw,
        |graph, res| {
            assert_nodes_have_ids(graph, &res.core_ids, vec![2, 3], true);
            assert_nodes_have_ids(graph, &res.non_core_ids, vec![5], false);
        },
    )
}
