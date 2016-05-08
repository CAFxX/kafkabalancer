package main

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
)

var steps = []func(*PartitionList, RebalanceConfig) (*PartitionList, error){
	ValidateWeights,
	FillDefaults,
	RemoveExtraReplicas,
	AddMissingReplicas,
	MoveDisallowedReplicas,
	MoveNonLeaders,
	MoveLeaders,
}

func emptypl() *PartitionList {
	return &PartitionList{Version: 1}
}

func singlepl(p Partition) *PartitionList {
	return &PartitionList{Version: 1, Partitions: []Partition{p}}
}

func replacepl(p Partition, orig BrokerID, repl BrokerID) *PartitionList {
	for idx, id := range p.Replicas {
		if id == orig {
			if repl == -1 {
				p.Replicas = append(p.Replicas[:idx], p.Replicas[idx+1:]...)
			} else {
				p.Replicas[idx] = repl
			}
			return singlepl(p)
		}
	}
	panic(fmt.Sprintf("partition %v replicas don't contain %d", p, orig))
}

// Balance analyzes the workload distribution among brokers for the
// partitions listed in the argument. It returns a PartitionList with 0 or more
// partition reassignments.
func Balance(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	log.Printf("rebalance config: %+v", cfg)

	for _, step := range steps {
		ppl, err := step(pl, cfg)
		if err != nil {
			stepName := runtime.FuncForPC(reflect.ValueOf(step).Pointer()).Name()
			return nil, fmt.Errorf("%s: %s", stepName, err)
		}
		if ppl != nil {
			return ppl, nil
		}
	}

	return emptypl(), nil
}

// ValidateWeights make sure that either all partitions have an explicit,
// non-negative weight or that all partitions have no weight
func ValidateWeights(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	hasWeights := pl.Partitions[0].Weight != 0

	for _, p := range pl.Partitions {
		if hasWeights && p.Weight == 0 {
			return nil, fmt.Errorf("partition %v has no weight", p)
		}
		if !hasWeights && p.Weight != 0 {
			return nil, fmt.Errorf("partition %v has weight", p)
		}
		if p.Weight < 0 {
			return nil, fmt.Errorf("partition %v has negative weight", p)
		}
	}

	return nil, nil
}

func FillDefaults(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	// if the weights are 0, set them to 1
	if pl.Partitions[0].Weight == 0 {
		for _, p := range pl.Partitions {
			p.Weight = 1.0
		}
	}

	// if the set of candidate brokers is empty, fill it with the default set
	brokers := cfg.Brokers
	if brokers == nil {
		brokers = getBrokerList(pl)
	}
	for _, p := range pl.Partitions {
		if p.Brokers == nil {
			p.Brokers = brokers
		}
	}

	// if the desired number of replicas is 0, fill it with the current number
	for _, p := range pl.Partitions {
		if p.NumReplicas == 0 {
			p.NumReplicas = len(p.Replicas)
		}
	}

	return nil, nil
}

func RemoveExtraReplicas(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	// remove unneeded replicas on existing brokers
	for _, p := range pl.Partitions {
		if p.NumReplicas < len(p.Replicas) {
			brokersByLoad := getBrokerListByLoad(pl, p.Brokers)
			replicaset := toBrokerSet(p.Replicas)
			for _, b := range brokersByLoad {
				if _, found := replicaset[b]; found {
					return replacepl(p, b, -1), nil
				}
			}
			return nil, fmt.Errorf("partition %v unable to pick replica to remove", p)
		}
	}

	return nil, nil
}

func AddMissingReplicas(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	// add missing replicas
	for _, p := range pl.Partitions {
		if p.NumReplicas > len(p.Replicas) {
			brokersByLoad := getBrokerListByLoad(pl, p.Brokers)
			replicaset := toBrokerSet(p.Replicas)
			for idx := len(brokersByLoad) - 1; idx >= 0; idx++ {
				b := brokersByLoad[idx]
				if _, found := replicaset[b]; !found {
					p.Replicas = append(p.Replicas, b)
					return singlepl(p), nil
				}
			}
			return nil, fmt.Errorf("partition %v unable to pick replica to add", p)
		}
	}

	return nil, nil
}

func MoveDisallowedReplicas(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	// move replicas on non-existing brokers to allowed ones
	for _, p := range pl.Partitions {
		brokersByLoad := getBrokerListByLoad(pl, p.Brokers)
		brokersset := toBrokerSet(brokersByLoad)

		for _, id := range p.Replicas {
			if _, allowed := brokersset[id]; allowed {
				continue
			}

			replicaset := toBrokerSet(p.Replicas)

			for idx := len(brokersByLoad) - 1; idx >= 0; idx++ {
				b := brokersByLoad[idx]
				if _, alreadyInReplicas := replicaset[b]; alreadyInReplicas {
					continue
				}

				return replacepl(p, id, b), nil
			}

			return nil, fmt.Errorf("partition %v unable to pick replica to replace broker %d", p, id)
		}
	}

	return nil, nil
}

func MoveNonLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	return nil, nil
}

func MoveLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	return nil, nil
}
