# vigil-case

This repository contains the resolution of a case passed by Vigil.
The goal is to read an S3 directory with `csv` and `tsv` files, which contain
two columns with random headers. The first column will always be a **key** and
the second one a **value**, both of them are integers.
For any given key across the entire dataset (so all the files), there exists
exactly one value that occurs an odd number of times.

There are going to be cases like this:
```
1,2
1,3
1,3
2,4
2,4
2,4
```

But never like this:
```
1 -> 2
1 -> 3
1 -> 4
```

Also, there may be some lines with missing values. In that case, we should replace them with 0.

Once the data is loaded, the keys that have a value that repeats an odd number of times
should be identified (as well as the respective value) and should be stored in a `tsv` file, also on S3.
The solution must be made using Apache Spark.

## The stack

For this challenge, I used Apache Spark and Spark SQL to develop solutions with
RDDs and Dataframes (respectively). I also added the Hadoop dependencies to handle
Amazon S3 filesystem.

To avoid the configuration and pricing over AWS services, I used a MinIO container
to simulate the S3 service (since the APIs are compatible).

## The setup

The project is self-contained, without the need of configuring Spark or Hadoop clusters.
To start the MinIO container, just run `./minio-docker-run.sh`. The credentials are `user` and `password`.
There is also a script in [LoadFilesToS3](./src/main/scala/LoadFilesToS3.scala) that uploads
the files which contain the test cases to MinIO, over the bucket named `test`.

## The solutions

The solution can be divided into the following main steps:

- load the files into some Spark structure;
- process the lines of the files to parse the `csv` or `tsv`, replacing the
  missing values by 0 and discarding the headers;
- group and count the key/value pairs and filter the odd ones
- save the result into a `tsv` file inside an S3 bucket.

Here, I present 2 solutions: one using RDDs and the other with DataFrames.

### Solution 1 - RDD

This solution was developed using the RDD API from Spark.
I started with this one because the way that we manipulate the data is more
similar to day-to-day Scala data structures, so a first solution could be developed
faster. The data is processed using some basic scala functions and the grouping and
counting are made using Map-Reduce.

However, it has a drawback: this implementation gets the amazon S3 objects input streams
and persists the content in memory. That is not a good approach for big data scenarios and
could be resolved by using Spark (non-structured) stream API or even by persisting the files
into the local storage and loading them into the RDD. But, since the goal here is the data processing algorithm, I abstracted that problem.

The solution is implemented at [Solution1](src/main/scala/Solution1.scala) and the tests at
[Solution1Tests](src/test/scala/Solution1Test.scala).

### Solution 2 - DataFrames
This solution was developed using the DataFrame API from Spark.
Since I got some time left from the first solution, I wanted to try the DataFrame API.
It has been a while since I used this, then I am kinda rusty. However, this API is
more efficient, and I thought that I needed to present a solution using it. The whole pipeline
is made using column transformations, no udf (*user-defined function*) was used.

For this solution, I needed to use the main method instead of inheriting from App.
That is because I defined some operations over the dataframe using regex, and
I needed to test them separately.

The solution is implemented at [Solution2](src/main/scala/Solution2.scala) and the tests at
[Solution2Tests](src/test/scala/Solution2Test.scala).

### Brief discussion about the solutions

The main differences between the solutions are the APIs used: RDD vs DataFrame. RDDs are a lower level
representation of the data, using Scala objects. It provides an easy and efficient way to process the data,
accepting structured and non-structured data. It also provides type safety at compile time, allowing some 
errors being caught sooner. However, it has no optimization engine for its operations.

The DataFrame provides a higher level of representation, organizing the data into named columns, similar to a
table on relational databases. It can also provide a schema to represent the data. This also allows data to be
easily loaded from known formats, like csv, json, database tables, etc. However, it does not provide type safety
at compile time, but it uses the Catalyst optimization engine to its transformations.

This challenge seems to be efficiently solved by both approaches. However, due the grouping operations, Dataframes
may perform slightly better that RDDs, but I cannot be certain of it, since RDDs use partitioning technics to
optimize the data spread along the cluster.

## The tests

I focused the tests implementations on the algorithm's steps. There are no tests for S3 communication.
The main test cases that involve the rules established at the challenge are centralized at the trait
[SolutionTest](src/test/scala/SolutionTest.scala). Since the rules are the same, regardless of the solution,
I thought it was better to centralize all the cases into one abstract test suite. To implement the test
cases to a solution, we just need to inherit from this suite and implement a function that will define how the lines of the files are going to be processed and return the result to be tested.

This approach not only is useful to avoid code replication but also allows us to implement specific test
cases for specific solutions when needed. For instance, Solution 1 (RDD) did not need any other tests
than the ones already present at SolutionTest (there is a suite just for the line parsing methods at
[ParseHelperTest](src/test/scala/utils/ParseHelperTest.scala)). However, I added some tests to validate, on Solution 2,
the columns transformations that involve regex, which is used to replace the empty strings to 0.
