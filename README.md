# Apache Spark with Python

## My Spark practice notes.

### spark_explain_plan
[spark_explain_plan notebook](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/spark_explain_plan.ipynb)

### DAG and explain plan
https://www.tutorialkart.com/apache-spark/dag-and-physical-execution-plan/
```bash
How Apache Spark builds a DAG and Physical Execution Plan ?
1. User submits a spark application to the Apache Spark.
2. Driver is the module that takes in the application from Spark side.
3. Driver identifies transformations and actions present in the spark application. These identifications are the tasks.
4. Based on the flow of program, these tasks are arranged in a graph like structure with directed flow of execution from task to task forming no loops in the graph (also called DAG). DAG is pure logical.
5. This logical DAG is converted to Physical Execution Plan. Physical Execution Plan contains stages.
6. Some of the subsequent tasks in DAG could be combined together in a single stage.
Based on the nature of transformations, Driver sets stage boundaries.
7. There are two transformations, namely 
a. narrow transformations : Transformations like Map and Filter that does not require the data to be shuffled across the partitions.
b. wide transformations : Transformations like ReduceByKey that does require the data to be shuffled across the partitions.

8. Transformation that requires data shuffling between partitions, i.e., a wide transformation results in stage boundary.
9. DAG Scheduler creates a Physical Execution Plan from the logical DAG. Physical Execution Plan contains tasks and are bundled to be sent to nodes of cluster.
```

### Catalyst optimizer
http://data-informed.com/6-steps-to-get-top-performance-from-the-changes-in-spark-2-0/
```bash
What is Catalyst? Catalyst is the name of Spark’s integral query optimizer and execution planner for Dataset/DataFrame.

Catalyst is where most of the “magic” happens to improve the execution speed of your code. But in any complex system, “magic” is unfortunately not good enough to always guarantee optimal performance. Just as with relational databases, it is valuable to learn a bit about exactly how the optimizer works in order to understand its planning and tune your applications.

In particular, Catalyst can perform sophisticated refactors of complex queries. However, almost all of its optimizations are qualitative and rule-based rather than quantitative and statistics-based. For example, Spark knows how and when to do things like combine filters, or move filters before joins. Spark 2.0 even allows you to define, add, and test out your own additional optimization rules at runtime. [1][2]

On the other hand, Catalyst is not designed to perform many of the common optimizations that RDBMSs have performed for decades, and that takes some understanding and getting used to.

For example, Spark doesn’t “own” any storage, so it does not build on-disk indexes, B-Trees, etc. (although its parquet file support, if used well, can get you some related features). Spark has been optimized for the volume, variety, etc. of big data – so, traditionally, it has not been designed to maintain and use statistics about a stable dataset. E.g., where an RDBMS might know that a specific filter will eliminate most records, and apply it early in the query, Spark 2.0 does not know this fact and won’t perform that optimization
```

###Catalyst, the optimizer and Tungsten, the execution engine!
https://db-blog.web.cern.ch/blog/luca-canali/2016-09-spark-20-performance-improvements-investigated-flame-graphs
```bash
*** Note in particular the steps marked with (*), they are optimized with who-stage code generation

Code generation is the key
The key to understand the improved performance is with the new features in Spark 2.0 for whole-stage code generation. 
```

###Deep dive into the new Tungsten execution engine
https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html
```bash
1. The explain() function in the expression below has been extended for whole-stage code generation. In the explain output, when an operator has a star around it (*), whole-stage code generation is enabled. In the following case, Range, Filter, and the two Aggregates are both running with whole-stage code generation. Exchange, however, does not implement whole-stage code generation because it is sending data across the network.

spark.range(1000).filter("id > 100").selectExpr("sum(id)").explain()

== Physical Plan ==
*Aggregate(functions=[sum(id#201L)])
+- Exchange SinglePartition, None
   +- *Aggregate(functions=[sum(id#201L)])
      +- *Filter (id#201L > 100)
         +- *Range 0, 1, 3, 1000, [id#201L]
		 
2. Vectorization
The idea here is that instead of processing data one row at a time, the engine batches multiples rows together in a columnar format, and each operator uses simple loops to iterate over data within a batch. Each next() call would thus return a batch of tuples, amortizing the cost of virtual function dispatches. These simple loops would also enable compilers and CPUs to execute more efficiently with the benefits mentioned earlier.
```

###Catalyst Optimizer
https://data-flair.training/blogs/spark-sql-optimization-catalyst-optimizer/
```bash
1. Fundamentals of Catalyst Optimizer
In the depth, Catalyst contains the tree and the set of rules to manipulate the tree. 
Trees
A tree is the main data type in the catalyst. A tree contains node object. For each node, there is a node
Rules
We can manipulate tree using rules. We can define rules as a function from one tree to another tree.
2. 
a. Analysis - Spark SQL Optimization starts from relation to be computed. It is computed either from abstract syntax tree (AST) returned by SQL parser or dataframe object created using API. 
b. Logical Optimization - In this phase of Spark SQL optimization, the standard rule-based optimization is applied to the logical plan. It includes constant folding, predicate pushdown, projection pruning and other rules. 
c. In this phase, one or more physical plan is formed from the logical plan, using physical operator matches the Spark execution engine. And it selects the plan using the cost model.
d. Code Generation -  It involves generating Java bytecode to run on each machine. Catalyst uses the special feature of Scala language, “Quasiquotes” to make code generation easier because it is very tough to build code generation engines.
```

### cost based optimization
https://databricks.com/blog/2017/08/31/cost-based-optimizer-in-apache-spark-2-2.html
```bash
*** Query Benchmark and Analysis
We took a non-intrusive approach while adding these cost-based optimizations to Spark by adding a global config spark.sql.cbo.enabled to enable/disable this feature. In Spark 2.2, this parameter is set to false by default. 
1. At its core, Spark’s Catalyst optimizer is a general library for representing query plans as trees and sequentially applying a number of optimization rules to manipulate them. 
2. A majority of these optimization rules are based on heuristics, i.e., they only account for a query’s structure and ignore the properties of the data being processed,
3. ANALYZE TABLE command
CBO relies on detailed statistics to optimize a query plan. To collect these statistics, users can issue these new SQL commands described below:
ANALYZE TABLE table_name COMPUTE STATISTICS
```

###  sigmod_spark_sql
http://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
```

```


![explain_plan_physical](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_physical.jpg)

![explain_plan_sql_vs_dataFrame](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_sql_vs_dataFrame.jpg)

![queryOptimization_hint](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/queryOptimization_hint.jpg)

![explain_plan_joins](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_joins.jpg)

![explain_plan_joinHint](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_joinHint.jpg)

![explain_plan_groupBy](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_groupBy.jpg)

![partition_rePartition](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/partition_rePartition.jpg)

![transformations_narrowVSwide](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/transformations_narrowVSwide.jpg)