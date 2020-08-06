Dachshund is a graph mining library written in Rust. It provides high performance data structures for multiple kinds of graphs, from simple undirected graphs to typed hypergraphs. Dachshund also provides algorithms for common tasks for graph mining and analysis, ranging from shortest paths to graph spectral analysis.

## Examples
Out of the box Dachshund is ready to use for clique mining. For instance:
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
1) there is a clique in the grpah with ID 0 (only graph provided)
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
