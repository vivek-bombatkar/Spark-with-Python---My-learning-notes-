# Apache Spark with Python

## My Spark practice notes.
Learning is a continuous thing, though I am using Spark from quite a long time now I never noted down my practice exercise yet. With this repo, I am documenting it!

I have used databricks free community cloude for this excercises, here the link: 
https://community.cloud.databricks.com/login.html

![Spark2_Structured_Streaming_myLearning_infoGraphics](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/Spark2_Structured_Streaming_myLearning_infoGraphics.jpg)

![Spark2_myLearning_inforGraphics](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/Spark2_myLearning_inforGraphics.jpg)

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

### Catalyst, the optimizer and Tungsten, the execution engine!
https://db-blog.web.cern.ch/blog/luca-canali/2016-09-spark-20-performance-improvements-investigated-flame-graphs
```bash
*** Note in particular the steps marked with (*), they are optimized with who-stage code generation

Code generation is the key
The key to understand the improved performance is with the new features in Spark 2.0 for whole-stage code generation. 
```

### Deep dive into the new Tungsten execution engine
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

### Catalyst Optimizer
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

### working-with-udfs-in-apache-spark
https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/
```bash
It’s important to understand the performance implications of Apache Spark’s UDF features.  Python UDFs for example (such as our CTOF function) result in data being serialized between the executor JVM and the Python interpreter running the UDF logic – this significantly reduces performance as compared to UDF implementations in Java or Scala.  Potential solutions to alleviate this serialization bottleneck include:

Accessing a Hive UDF from PySpark as discussed in the previous section.  The Java UDF implementation is accessible directly by the executor JVM.  Note again that this approach only provides access to the UDF from the Apache Spark’s SQL query language.
Making use of the approach also shown to access UDFs implemented in Java or Scala from PySpark, as we demonstrated using the previously defined Scala UDAF example.

Another important component of Spark SQL to be aware of is the Catalyst query optimizer. Its capabilities are expanding with every release and can often provide dramatic performance improvements to Spark SQL queries; however, arbitrary UDF implementation code may not be well understood by Catalyst (although future features[3] which analyze bytecode are being considered to address this).  As such, using Apache Spark’s built-in SQL query functions will often lead to the best performance and should be the first approach considered whenever introducing a UDF can be avoided
```

### spark-functions-vs-udf-performance
https://stackoverflow.com/questions/38296609/spark-functions-vs-udf-performance


![explain_plan_physical](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_physical.jpg)

![explain_plan_sql_vs_dataFrame](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_sql_vs_dataFrame.jpg)

![queryOptimization_hint](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/queryOptimization_hint.jpg)

![explain_plan_joins](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_joins.jpg)

![explain_plan_joinHint](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_joinHint.jpg)

![explain_plan_groupBy](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/explain_plan_groupBy.jpg)

![partition_rePartition](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/partition_rePartition.jpg)

![transformations_narrowVSwide](https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/transformations_narrowVSwide.jpg)


### spark-submit
> https://spark.apache.org/docs/2.2.0/submitting-applications.html
> https://www.cloudera.com/documentation/enterprise/5-4-x/topics/cdh_ig_running_spark_on_yarn.html
> https://jaceklaskowski.gitbooks.io/mastering-apache-spark/yarn/
> https://blog.cloudera.com/blog/2014/05/apache-spark-resource-management-and-yarn-app-models/

```
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]

--class: The entry point for your application (e.g. org.apache.spark.examples.SparkPi)
--master: The master URL for the cluster (e.g. spark://23.195.26.187:7077)
--deploy-mode: Whether to deploy your driver on the worker nodes (cluster) or locally as an external client (client) (default: client) †
--conf: Arbitrary Spark configuration property in key=value format. For values that contain spaces wrap “key=value” in quotes (as shown).
application-jar: Path to a bundled jar including your application and all dependencies. The URL must be globally visible inside of your cluster, for instance, an hdfs:// path or a file:// path that is present on all nodes.
application-arguments: Arguments passed to the main method of your main class, if any
```

#### java vs python code execution

| java class | python script |
| --- | --- |
| --class 'class path of java main application' | (at the end of spark-submit) 'fully qualified path of the main python script' |
| ex. --class com.abc.project1.Main | /opt/src/project1/module1/main.py 'pass the parameters' |
| --jars 'assembly jar (or “uber” jar) containing your code and its dependencies,  to be distributed with your application' | --py-files 'add .py, .zip or .egg files to be distributed with your application.' |

#### <master-url>

| local | local[n] | local[n,f] | yarn |
| --- | --- | --- | --- | 
| Run locally with one worker thread, no parallelism  | Run locally with K worker threads , set this to the number of cores. local[*] Run with as many worker threads as logical cores  |  Run Spark locally with n worker threads and F maxFailures |  Connect to a YARN cluster in client or cluster mode depending on the value of --deploy-mode. The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable |

#### YARN client vs cluster
> Deploy modes are all about where the Spark driver runs.

| YARN client | YARN cluster | 
| --- | --- |
| driver runs on the host where the job is submitted |  the driver runs in the ApplicationMaster on a cluster host chosen by YARN. | 
| client that launches the application needs to be alive  | clientdoesn't need to continue running for the entire lifetime of the application |
|  <img src="https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/spark-yarn-client.png" width="200" height="200" /> | <img src="https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/yarn-cluster.png" width="200" height="200" /> |


<img src="https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/spark-yarn-table.png" width="500" height="300" />


| Spark local mode | Spark Cluster mode |
| --- | --- |
|  <img src="https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/spark_local.png" width="200" height="200" /> | <img src="https://github.com/vivek-bombatkar/Spark-with-Python---My-learning-notes-/blob/master/pics/spark_cluster.png" width="200" height="200" /> |

### Drivers and Executors

| IMP Concepts |   | 
|--- |--- |
| **Application** | single job, a sequence of jobs, a long-running service issuing new commands as needed or an interactive exploration session.|
| **Spark Driver** | driver is the process running the spark context. This driver is responsible for converting the application to a directed graph of individual steps to execute on the cluster. There is one driver per application.
| **Spark Application Master** | responsible for negotiating resource requests made by the driver with YARN and finding a suitable set of hosts/containers in which to run the Spark applications. There is one Application Master per application. |
| Spark Executor | A single JVM instance on a node that serves a single Spark application. An executor runs multiple tasks over its lifetime, and multiple tasks concurrently. A node may have several Spark executors and there are many nodes running Spark Executors for each client application. | 
| Spark Task | represents a unit of work on a partition of a distributed dataset.  |


### Spark job monitoring
Spark History Server web UI


### 


