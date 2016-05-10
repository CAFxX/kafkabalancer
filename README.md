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

## Installation
`go get github.com/cafxx/kafkabalancer`

## Usage
Run `kafkabalancer -help` for usage instructions.

```
Usage of ./kafkabalancer:
  -allow-leader
    	Consider the partition leader eligible for rebalancing
  -broker-ids string
    	Comma-separated list of broker IDs (default "auto")
  -input string
    	File to read
  -input-json
    	Parse the input as JSON
  -min-replicas int
    	Minimum number of replicas for a partition to be eligible for rebalancing (default 2)
  -min-umbalance float
    	Minimum umbalance value required to perform rebalancing (default 1e-05)
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
- minimize leader unbalance (maximize global throughput)
- minimize same-broker colocation of partitions of the same topic (maximize
  per-topic throughput)
- proactively minimize unbalance caused by broker failure
- consider N-way rebalancing plans (e.g. swap two replicas)

## Scenarios
This section lists some examples of how kafkabalancer operates.

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
Two partitions with the following set of replicas: [1 2], [1]
- remove one replica from the first partition: kafkabalancer will yield [2], [1]

### Automated rebalancing
Three partition with the following set of replicas: [1 2 3], [1 2 4], [1 2 3]
- kafkabalancer will yield [1 4 3], [1 2 4], [1 2 3]

## Author
Carlo Alberto Ferraris

## License
[MIT](LICENSE)
