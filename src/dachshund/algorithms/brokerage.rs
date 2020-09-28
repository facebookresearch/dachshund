/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
use crate::dachshund::graph_base::GraphBase;
use crate::dachshund::id_types::NodeId;
use crate::dachshund::node::{DirectedNodeBase, NodeEdgeBase};
use std::collections::HashMap;

pub struct BrokerageScores {
    pub num_coordinator_ties: usize,
    pub num_itinerant_broker_ties: usize,
    pub num_gatekeeper_ties: usize,
    pub num_representative_ties: usize,
    pub num_liaison_ties: usize,
    pub total_open_twopaths: usize,
}

pub trait Brokerage: GraphBase
where
    Self: GraphBase,
    <Self as GraphBase>::NodeType: DirectedNodeBase,
{
    fn get_brokerage_scores_for_node(
        &self,
        node_id: NodeId,
        community_membership: &HashMap<NodeId, usize>,
    ) -> BrokerageScores {
        let mut scores = BrokerageScores {
            num_coordinator_ties: 0,
            num_itinerant_broker_ties: 0,
            num_gatekeeper_ties: 0,
            num_representative_ties: 0,
            num_liaison_ties: 0,
            total_open_twopaths: 0,
        };
        let c_v = community_membership.get(&node_id).unwrap();
        let node: &<Self as GraphBase>::NodeType = self.get_node(node_id);
        for a in node.get_in_neighbors() {
            let a_id = a.get_neighbor_id();
            let a_node = self.get_node(a_id);
            let c_a = community_membership.get(&a_id).unwrap();
            for b in node.get_out_neighbors() {
                let b_id = b.get_neighbor_id();
                if !a_node.has_out_neighbor(b_id) {
                    let c_b = community_membership.get(&b_id).unwrap();
                    if c_v == c_a && c_v == c_b {
                        scores.num_coordinator_ties += 1;
                    } else if c_v != c_a && c_a == c_b {
                        scores.num_itinerant_broker_ties += 1;
                    } else if c_v != c_a && c_v == c_b {
                        scores.num_gatekeeper_ties += 1;
                    } else if c_v == c_a && c_v != c_b {
                        scores.num_representative_ties += 1;
                    } else {
                        assert!(c_v != c_a && c_v != c_b);
                        scores.num_liaison_ties += 1;
                    }
                    scores.total_open_twopaths += 1;
                }
            }
        }
        scores
    }
}
