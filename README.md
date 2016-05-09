# kafkabalancer
Rebalance your kafka topics, partitions, replicas across your cluster

## Purpose
kafkabalancer allows you to compute the set of rebalancing operations yielding a
minimally-unbalanced kafka cluster, given a set of constraints:

- set of allowed brokers (globally, or per partition)
- number of desired replicas (per partition)
- current distribution of replicas (per partition)
- leader reassignment enabled/disabled (globally)
- partition weight (per partition)

The goal is to minimize the workload difference between brokers in the cluster,
where the workload of a broker is measured by the sum of the weights of each
partition having a replica on that broker.

The tool is designed to be used iteratively: at each iteration only a single
reassignment operation is returned by kafkabalancer. This is useful in a
automated framework like the following:

```
  forever:
    if !cluster_is_nominal:
      continue
    state = get_current_state()
    change = kafkabalancer(state)
    if change:
      apply change
```    

## Features
- parse the output of kafka-topic.sh --describe
- parse the reassignment JSON format
- output the reassignment JSON format

### Planned
- parse the output of kafka-offset.sh to get the per-partiton weights (number of
  messages)
- fetch elsewhere additional metrics to refine the weights (e.g. number of
  consumers, size of messages)
- proactively minimize unbalance caused by broker failure

## Scenarios

### Adding brokers
Two partitions with the following set of replicas: [1 2], [2 1].
- add one broker: kafkabalancer will yield [1 3], [2 1]
- add two brokers: kafkabalancer will yield [4 3], [2 1]

### Removing brokers
Three partitions with the following set of replicas: [1 2], [1], [3].
- remove one broker: kafkabalancer will yield [1 2], [1], [2]
- remove two brokers: kafkabalancer will return error (first partition needs two
  brokers)

### Add replicas
Three partitions with the following set of replicas: [1 2], [1 3], [3].
- add one replica to the third partition: kafkabalancer will yield [1 2], [1 3], [2 3]

### Remove replicas
(TBD)

### Automated rebalancing
(TBD)

## Author
Carlo Alberto Ferraris
