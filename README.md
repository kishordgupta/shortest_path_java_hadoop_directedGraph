# shortest_path_java_hadoop_directedGraph
compute the shortest path distance between two given nodes in a directed unweighted
graph, named \direct.graph". The shortest path distance from node a to node b is the minimum path length
from a to b.
Input
\direct.graph" is stored in the edge list format. Each line contains two numbers a b, representing a directed
edge from a to b. For example, a sample graph le \direct.graph" contains the following lines:
1 2
1 3
1 4
2 3
5 2 5
3 1
3 2
4 2
4 5
Note: SNAP is a collection of real-world graph data available for download. You can use these graphs for
practices and testing.
Besides \direct.graph", your program should read from another input le named \DTest" that contains one
or multiple lines of node pairs for the shortest path distance query. Each line is in a b format, for example:
2 4
4 3
Note: You can assume all node ids listed in \DTest" are valid.
Output
The output of your program should be a le named \DRes", where the number on each line corresponding
to the shortest path distance of querying nodes (following the same order). For example, following the above
sample graph and queries, your output le should contain
Page 2 of 5
COMP7/8991 - Spring 2019 : Assignments
3
2
Note: It is possible that two query nodes are not reachable, in this case you should output 􀀀1
