package main

import "fmt"

// ValidateWeights make sure that either all partitions have an explicit,
// strictly positive weight or that all partitions have no weight
func ValidateWeights(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
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

// ValidateReplicas checks that partitions don't have more than one replica per
// broker
func ValidateReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	for _, p := range pl.Partitions {
		replicaset := toBrokerSet(p.Replicas)
		if len(replicaset) != len(p.Replicas) {
			return nil, fmt.Errorf("partition %v has duplicated replicas", p)
		}
	}

	return nil, nil
}

// FillDefaults fills in default values for Weight, Brokers and NumReplicas
func FillDefaults(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	// if the weights are 0, set them to 1
	if pl.Partitions[0].Weight == 0 {
		for idx := range pl.Partitions {
			pl.Partitions[idx].Weight = 1.0
		}
	}

	// if the set of candidate brokers is empty, fill it with the default set
	brokers := cfg.Brokers
	if brokers == nil {
		brokers = getBrokerList(pl)
	}
	for idx := range pl.Partitions {
		if pl.Partitions[idx].Brokers == nil {
			pl.Partitions[idx].Brokers = brokers
		}
	}

	// if the desired number of replicas is 0, fill it with the current number
	for idx := range pl.Partitions {
		if pl.Partitions[idx].NumReplicas == 0 {
			pl.Partitions[idx].NumReplicas = len(pl.Partitions[idx].Replicas)
		}
	}

	return nil, nil
}

// RemoveExtraReplicas removes replicas from partitions having lower NumReplicas
// than the current number of replicas
func RemoveExtraReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)

	for _, p := range pl.Partitions {
		if p.NumReplicas >= len(p.Replicas) {
			continue
		}

		brokersByLoad := getBrokerListByLoad(loads, p.Brokers)
		replicaset := toBrokerSet(p.Replicas)
		for _, b := range brokersByLoad {
			if _, found := replicaset[b]; found {
				return replacepl(p, b, -1), nil
			}
		}

		return nil, fmt.Errorf("partition %v unable to pick replica to remove", p)
	}

	return nil, nil
}

// AddMissingReplicas adds replicas to partitions having NumReplicas greater
// than the current number of replicas
func AddMissingReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)
	// add missing replicas
	for _, p := range pl.Partitions {
		if p.NumReplicas <= len(p.Replicas) {
			continue
		}

		brokersByLoad := getBrokerListByLoad(loads, p.Brokers)
		replicaset := toBrokerSet(p.Replicas)
		for idx := len(brokersByLoad) - 1; idx >= 0; idx-- {
			b := brokersByLoad[idx]
			if _, found := replicaset[b]; !found {
				return addpl(p, b), nil
			}
		}

		return nil, fmt.Errorf("partition %v unable to pick replica to add", p)
	}

	return nil, nil
}

// MoveDisallowedReplicas moves replicas from non-allowed brokers to the least
// loaded ones
func MoveDisallowedReplicas(pl *PartitionList, _ RebalanceConfig) (*PartitionList, error) {
	loads := getBrokerLoad(pl)

	for _, p := range pl.Partitions {
		brokersByLoad := getBrokerListByLoad(loads, p.Brokers)
		brokersset := toBrokerSet(brokersByLoad)

		for _, id := range p.Replicas {
			if _, allowed := brokersset[id]; allowed {
				continue
			}

			replicaset := toBrokerSet(p.Replicas)

			for idx := len(brokersByLoad) - 1; idx >= 0; idx-- {
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

func move(pl *PartitionList, cfg RebalanceConfig, leaders bool) (*PartitionList, error) {
	var cp Partition
	var cr, cb BrokerID

	loads := getBrokerLoad(pl)
	for _, id := range cfg.Brokers {
		if _, found := loads[id]; !found {
			loads[id] = 0
		}
	}

	su := getUnbalance(loads)
	cu := su

	for _, p := range pl.Partitions {
		if p.NumReplicas < cfg.MinReplicasForRebalancing {
			continue
		}

		replicaset := toBrokerSet(p.Replicas)
		replicas := p.Replicas[1:]
		if leaders {
			replicas = p.Replicas[0:1]
		}

		for _, r := range replicas {
			rload := loads[r]
			loads[r] -= p.Weight

			for _, b := range p.Brokers {
				if _, alreadyInReplicas := replicaset[b]; alreadyInReplicas {
					continue
				}

				bload := loads[b]
				loads[b] += p.Weight
				u := getUnbalance(loads)
				if u < cu {
					cu, cp, cr, cb = u, p, r, b
				}

				loads[b] = bload
			}

			loads[r] = rload
		}
	}

	if cu < su-cfg.MinUnbalance {
		return replacepl(cp, cr, cb), nil
	}

	return nil, nil
}

// MoveNonLeaders moves non-leader replicas from overloaded brokers to
// underloaded brokers
func MoveNonLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	return move(pl, cfg, false)
}

// MoveLeaders moves leader replicas from overloaded brokers to underloaded
// brokers
func MoveLeaders(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	if !cfg.AllowLeaderRebalancing {
		return nil, nil
	}

	return move(pl, cfg, true)
}
