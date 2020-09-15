Dachshund is a graph mining library written in Rust. It provides high performance data structures for multiple kinds of graphs, from simple undirected graphs to typed hypergraphs. Dachshund also provides algorithms for common tasks for graph mining and analysis, ranging from shortest paths to graph spectral analysis.

## Examples

### Graph featurizer
This application takes a list of graphs and featurizes it. For instance:

```
cat example.txt | cut -s -f1-3 | target/debug/simple_graph_featurizer
```
The output will look like this:

```
0	{"bet_cent":1.2,"clust_coef":0.0,"evcent":0.868,"num_16_cores":0,"num_17_trusses":0,"num_2_cores":1,"num_3_trusses":0,"num_4_cores":0,"num_5_trusses":0,"num_8_cores":0,"num_9_trusses":0,"num_connected_components":1,"num_edges":5,"size_of_largest_cc":5}
```
What the various JSON-encoded features mean:
- `bet_cent`: average betweenness centrality.
- `clust_coef`: average clustering coefficient.
- `evcent`: average eigenvector centrality.
- `num_{k}_cores`: {k}-core count.
- `num_{k}_trusses`: {k}-truss count.
- `num_connected_components`: number of connected components.
- `num_edges`: number of edges.
- `size_of_largest_cc`: number of nodes in largest connected component.

### Clique miner
This application finds the largest (quasi-) cliques in a graph. For instance:
```
cargo build
cat example.txt | target/debug/clique_miner \
  --typespec '[["author", "published", "article"]]' \
  --beam_size 20 --alpha 0.1 --global_thresh 1.0  \
  --local_thresh 1.0 --num_to_search 10 --epochs 200 \
  --max_repeated_prior_scores 3 --debug_mode false \
  --min_degree 1 --core_type author --long_format false
```

The output should look like this:

```
0	2	2	[1,2]	[3,4]	["article","article"]	1	[1.0,1.0]	[1.0]
```
What this means:
1) there is a clique in the graph with ID 0 (only graph provided)
2) the clique has core nodes (authors) 1 and 2
3) the clique has non-core nodes 3 and 4
4) both non-core nodes are articles
5) the global density is 1 (all edges that could exist do exist)
6) the local density for each of the two core nodes is 1.0
7) the density for the one non-core type ("article") is 1.0

For a better explanation of what the various arguments mean:
```
target/debug/clique_miner --help
```

To run various tests:
```
cargo test
```

## Requirements
All requirements are handled by cargo.

## Building Dachshund
Simply run `cargo build`. The executable should show up in `target/debug/clique_miner`.

## How Dachshund works
The clique miner is the first dachshund application. It uses a beam search algorithm (plus some other optimizations) to find the largest (quasi-)cliques it can find. It supports initialization with known clique solutions.

See `./target/debug/clique_miner --help` for meaning of each option.

See the [CONTRIBUTING](CONTRIBUTING.md) file for how to help out.

## License
Dachshund is MIT licensed, as found in the LICENSE file.
