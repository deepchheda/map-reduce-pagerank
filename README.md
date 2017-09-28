# PageRank in MapReduce exploring the behavior of iterative graph algorithm.

Applied PageRank algorithm to Wikipedia articles in order to find the
top 100 most referenced links.
The data dump was taken as a Hadoop-friendly .bz2-compressed files.
A smaller version of this data is uploaded on Github.

The data was converted into an adjacency-list based graph representation after
cleaning the data.

To implement PageRank, I implemented a series of iterative jobs, the first one
is to perform the preprocessing and then 10 jobs to refine the PageRanks
after which, the top 100 Links and their PageRanks are written to the part file
in the output folder by the last job.

This is a maven project with a Makefile so it will run in any environment by
running the following commands on the terminal:

To run locally:
make switch-standalone
make alone

To run on AWS:
make upload-input-aws
make cloud
