=====================
Hardware Requirements
=====================

**Memory**

It is recommended to allocate at least 16 GB of RAM per node for Presto.

**Other Recommendations**
Presto can be installed on any normally configured Hadoop cluster. YARN should
be configured to account for resources dedicated to Presto. For example, if a
node has 64GB of RAM, perhaps you would normally allocate 60GB to YARN.  If you
install Presto on that node and give Presto 16GB of RAM, then you should
subtract 16GB from the 60GB and let YARN only allocate 44GB per node. An
optimized configuration might choose to have separate Presto and Hadoop nodes.
The optimized configuration allows you to give more memory to Presto, and thus
perform larger join queries, for example.
