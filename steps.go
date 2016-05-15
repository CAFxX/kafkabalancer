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
		for _, b := range brokersByLoad {
			if inBrokerList(p.Replicas, b) {
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
		for idx := len(brokersByLoad) - 1; idx >= 0; idx-- {
			b := brokersByLoad[idx]
			if !inBrokerList(p.Replicas, b) {
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
	bl := getBL(loads)

	for _, p := range pl.Partitions {
		brokersByLoad := getBrokerListByLoadBL(bl, p.Brokers)

		for _, id := range p.Replicas {
			if inBrokerList(brokersByLoad, id) {
				continue
			}

			for idx := len(brokersByLoad) - 1; idx >= 0; idx-- {
				b := brokersByLoad[idx]
				if inBrokerList(p.Replicas, b) {
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

	bl := getBL(loads)
	su := getUnbalanceBL(bl)
	cu := su

	for _, p := range pl.Partitions {
		if p.NumReplicas < cfg.MinReplicasForRebalancing {
			continue
		}

		replicas := p.Replicas[1:]
		if leaders {
			replicas = p.Replicas[0:1]
		}

		for _, r := range replicas {
			ridx := -1
			var rload float64
			for idx, b := range bl {
				if b.ID == r {
					ridx = idx
					rload = b.Load
					bl[idx].Load -= p.Weight
				}
			}
			if ridx == -1 {
				return nil, fmt.Errorf("assertion failed: replica %d not in broker loads %v", r, bl)
			}

			for idx, b := range bl {
				if !inBrokerList(p.Brokers, b.ID) {
					continue
				}
				if inBrokerList(p.Replicas, b.ID) {
					continue
				}

				bload := bl[idx].Load
				bl[idx].Load += p.Weight
				u := getUnbalanceBL(bl)
				if u < cu {
					cu, cp, cr, cb = u, p, r, b.ID
				}

				bl[idx].Load = bload
			}

			bl[ridx].Load = rload
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
