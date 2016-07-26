# Predictive maintenance using Spark

It is educational project with synthetically generated data.

The main idea is to use current measurements along with several previous to generate feature vector.

## Issues

1 It can detect failure state several times per device. Some logic should be added if needed.

2 Since data is synthetic no algrorithm selection was done. Basic logistic regression was used and count
    of iterations was determined by also generated test dataset

3 There is a shuffling phase in prediction app. It can be removed by using, for example, http://spark.apache.org/docs/latest/streaming-kafka-integration.html#approach-2-direct-approach-no-receivers

4 Streaming integration tests are ugly and fragile (since shared variable is used). Some machinery with DSL should be added
