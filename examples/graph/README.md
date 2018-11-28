# BigSparse / GraFBoost

External graph analytics platform using the sort-reduce library

More details about the sort-reduce algorithm, as well as its application to graph analytics, can be seen in the paper [GraFBoost: Accelerated Flash Storage for External Graph Analytics](http://people.csail.mit.edu/wjun/papers/isca2018-camera.pdf), and [BigSparse: High-performance external graph analytics](https://arxiv.org/abs/1710.07736)

## Building the software

After the sort-reduce library has been built, simply run **make** to compile the example algorithms, bfs (breadth-first-search), pr (pagerank), and bc (betweenness-centrality)

## Caveats in the current version

PR doesn't have active vertex selection implemented (the whole graph is processed every iteration right now).
There is no reason active vertex selection cannot be done using sort-reduce (It is mentioned in the paper), but that requires an additional row-compressed graph file which I have not created yet.
In order to do performance comparisons, PR should only be run for 1 iteration.

## Graph file format

This software expects a graph to be a binary sparse compressed column format. 

A graph consists of two files: an index file and a matrix file.

For a graph with **V** vertices, the index file consists of **V+1** 64 bit values.
Each i'th value corresponds to the byte offset of the vertex i's outbound edge data, stored in the matrix file.
So if i'th value is 8 and (i+1)'st value is 16, bytes [8,16) contain the outbound edge data of the graph.

The outbound edge data can either be a sequence of destination vertex IDs, or a pair of ID, weights. 
The provided examples expects a sequence of 4 byte destination vertex IDs, so in the above example, [8,16) bytes will contain two destination vertex IDs.

### Encoding graphs

*utils/encode* takes as input a text-encoded graph file consisting of source-destination pairs, and creates the index and matrix files required by this program.
Such text-encoded graph files include the famous [twitter dataset](http://an.kaist.ac.kr/traces/WWW2010.html).

*encode* takes as input the input filename, whether the vertex ids should be encoded in 32 or 64 bit format, and whether the input is a text file.
In order to encode input.txt, using 32 bit vertex IDs, run the following command:

```sh
$ ./encode input.txt Y Y # First Y is for 32 bits? and second Y is for text input?
```

