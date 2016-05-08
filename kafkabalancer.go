package main

import (
	"flag"
	"log"
	"os"
)

type BrokerID int
type PartitionID int
type TopicName string

type PartitionList struct {
	Version    int         `json:"version"`
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	Topic     TopicName   `json:"topic"`
	Partition PartitionID `json:"partition"`
	Replicas  []BrokerID  `json:"replicas"`
	// extensions
	Weight      float64    `json:"weight,omitempty"`       // default: 1.0
	NumReplicas int        `json:"num_replicas,omitempty"` // default: len(replicas)
	Brokers     []BrokerID `json:"brokers,omitempty"`      // default: (auto)
}

var jsonInput = flag.Bool("input-json", false, "Parse the input as JSON")
var input = flag.String("input", "", "File to read")

var allowLeader = flag.Bool("allow-leader", DefaultRebalanceConfig().AllowLeaderRebalancing, "Consider the partition leader eligible for rebalancing")
var minReplicas = flag.Int("min-replicas", DefaultRebalanceConfig().MinReplicasForRebalancing, "Minimum number of replicas for a partition to be eligible for rebalancing")
var minUnbalance = flag.Float64("min-umbalance", DefaultRebalanceConfig().MinUnbalance, "Minimum umbalance value required to perform rebalancing")

var brokerIDs = flag.String("broker-ids", "auto", "Comma-separated list of broker IDs")

func apply(orig *PartitionList, change *PartitionList) {
PartitionLoop:
	for idx, p := range orig.Partitions {
		for _, q := range change.Partitions {
			if p.Topic == q.Topic && p.Partition == q.Partition {
				orig.Partitions[idx].Replicas = q.Replicas
				continue PartitionLoop
			}
		}
	}
}

func main() {
	flag.Parse()
	var err error

	in := os.Stdin
	if *input != "" {
		in, err = os.Open(*input)
		if err != nil {
			log.Printf("failed opening file %s: %s", *input, err)
			os.Exit(1)
		}
		defer in.Close()
	}

	out := os.Stdout

	pl, err := ParsePartitionList(in, *jsonInput)
	if err != nil {
		log.Printf("failed parsing partition list: %s", err)
		os.Exit(2)
	}

	for {
		ppl, err := Balance(pl, RebalanceConfig{
			AllowLeaderRebalancing:    *allowLeader,
			MinReplicasForRebalancing: *minReplicas,
			MinUnbalance:              *minUnbalance,
		})
		if err != nil {
			log.Printf("failed optimizing distribution: %s", err)
			os.Exit(3)
		}

		if len(ppl.Partitions) == 0 {
			os.Exit(0)
		}

		err = WritePartitionList(out, ppl)
		if err != nil {
			log.Printf("failed writing partition list: %s", err)
			os.Exit(4)
		}
	}
}
