package main

import (
	"fmt"
	"log"
	"sort"
)

type RebalanceConfig struct {
	AllowLeaderRebalancing    bool
	MinReplicasForRebalancing int
	MinUnbalance              float64

	Brokers []BrokerID
}

func DefaultRebalanceConfig() RebalanceConfig {
	return RebalanceConfig{
		AllowLeaderRebalancing:    false,
		MinReplicasForRebalancing: 2,
		MinUnbalance:              0.0005,
	}
}

type BrokerLoad struct {
	ID   BrokerID
	Load float64
}

// ComputeUnbalance computes a (made-up) metric that measures how much the
// workload is unbalanced across the brokers in the kafka cluster. Specifically,
// the unbalance is higher the more brokers have over-average workloads.
func ComputeUnbalance(brokerLoads []BrokerLoad) float64 {
	var sumBrokerLoad float64
	var maxBrokerLoad float64

	for _, b := range brokerLoads {
		sumBrokerLoad += b.Load
		if maxBrokerLoad < b.Load {
			maxBrokerLoad = b.Load
		}
	}

	avgBrokerLoad := sumBrokerLoad / float64(len(brokerLoads))

	var brokerUnbalance float64
	for _, b := range brokerLoads {
		relBrokerLoad := b.Load/avgBrokerLoad - 1.0
		if relBrokerLoad > 0 {
			brokerUnbalance += relBrokerLoad * relBrokerLoad
		} else {
			brokerUnbalance += relBrokerLoad * relBrokerLoad / 2
		}
	}

	return brokerUnbalance
}

func getBrokerLoads(pl *PartitionList) []BrokerLoad {
	brokerLoads := make(map[BrokerID]float64)

	// calculate the load on each broker
	for _, partition := range pl.Partitions {
		for _, brokerID := range partition.Replicas {
			brokerLoads[brokerID] += partition.Weight
		}
	}

	var BrokerLoads []BrokerLoad
	for brokerID, brokerLoad := range brokerLoads {
		BrokerLoads = append(BrokerLoads, BrokerLoad{ID: brokerID, Load: brokerLoad})
	}
	sort.Sort(ByID(BrokerLoads))

	return BrokerLoads
}

// AnalyzeDistribution analyzes the workload distribution among brokers for the
// partitions listed in the argument. It returns a PartitionList with 0 or more
// partition reassignments.
func AnalyzeDistribution(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	log.Printf("rebalance config: %+v", cfg)

	BrokersByID := getBrokerLoads(pl)

	numBrokers := len(BrokersByID)
	switch numBrokers {
	case 0:
		return nil, fmt.Errorf("no brokers")
	case 1:
		// TODO: handle changing the number of brokers
		return nil, fmt.Errorf("only 1 broker found")
	}

	// compute the current unbalance
	brokerUnbalance := ComputeUnbalance(BrokersByID)
	log.Printf("broker loads: %v", BrokersByID)
	log.Printf("unbalance: %.3f", brokerUnbalance)

	// if unbalance is less than the min threshold simply return
	if brokerUnbalance <= cfg.MinUnbalance {
		return &PartitionList{Version: 1}, nil
	}

	// find the best replica to move
	// goal 1 - minimize unbalance between brokers
	// goal 2 - minimize client disruption (prefer topics with more replicas)
	// goal 3 - minimize cluster disruption (prefer fewer operations)
	// current strategy: pick the replica from a "overloaded" broker and move it
	// somewhere else to minimize the unbalance metric
	// TODO: handle multi-way moves (i.e. consider also swapping two replicas
	//       instead of just moving one replica from one node to the other)
	// TODO: we likely want to minimize partition colocation on the same broker
	// TODO: minimize unbalance also when 1 or more brokers are missing
	minUnbalance := brokerUnbalance
	var minUnbalancePartition Partition
	var minUnbalanceBrokerID BrokerID = -1
	var minUnbalanceBrokerOrigID BrokerID

	for maxBrokerIdx, maxBroker := range BrokersByID {
		for _, partition := range pl.Partitions {
			// first let's make sure that this partition is eligible for rebalancing:
			// the partition needs to have enough replicas
			if len(partition.Replicas) < cfg.MinReplicasForRebalancing {
				continue
			}

			// one of the replicas of the partition should be on the broker and, if we
			// don't allow leader replicas to be moved, it should not be a leader
			onMaxLoadBroker := false
			for idx, replica := range partition.Replicas {
				if replica == maxBroker.ID {
					if cfg.AllowLeaderRebalancing == false && idx == 0 {
						break
					}
					onMaxLoadBroker = true
					break
				}
			}
			if !onMaxLoadBroker {
				continue
			}

			// we now that the current partition is eligible for relocation: now let's
			// simulate what the unbalance would be if we would move the replica on the
			// highest-loaded broker to a different broker
			BrokersByID[maxBrokerIdx].Load -= partition.Weight

		brokerLoop:
			for idx, b := range BrokersByID {
				// we can't move the replica to a broker that already contains a replica
				// of the same partition (this includes the highest-loaded broker!)
				for _, replica := range partition.Replicas {
					if b.ID == replica {
						continue brokerLoop
					}
				}

				// ok, now compute the unbalance after moving the replica: if it's lower
				// than the best unbalance found so far remember this as the candidate
				// rebalancing operation
				BrokersByID[idx].Load += partition.Weight
				unbalance := ComputeUnbalance(BrokersByID)
				if minUnbalance > unbalance {
					minUnbalance = unbalance
					minUnbalancePartition = partition
					minUnbalanceBrokerID = b.ID
					minUnbalanceBrokerOrigID = maxBroker.ID
					log.Printf("partition %v broker %d->%d: unbalance %.3f", partition, maxBroker.ID, b.ID, unbalance)
				}
				BrokersByID[idx].Load -= partition.Weight
			}

			BrokersByID[maxBrokerIdx].Load += partition.Weight
		}
	}

	// if we couldn't find anything to relocate simply return an empty list
	if minUnbalanceBrokerID == -1 {
		return &PartitionList{Version: 1}, nil
	}

	// if the delta between previous unbalance and new unbalance is lower than
	// the threshold simply return an empty list
	if brokerUnbalance-minUnbalance <= cfg.MinUnbalance {
		return &PartitionList{Version: 1}, nil
	}

	res := &PartitionList{
		Version:    1,
		Partitions: []Partition{minUnbalancePartition},
	}
	for idx, brokerID := range res.Partitions[0].Replicas {
		if brokerID == minUnbalanceBrokerOrigID {
			res.Partitions[0].Replicas[idx] = minUnbalanceBrokerID
			res.Partitions[0].Weight = 0

			log.Printf("relocations %v: unbalance %.3f", res.Partitions, minUnbalance)
			return res, nil
		}
	}

	return nil, fmt.Errorf("failed optimization")
}

type ByID []BrokerLoad

func (a ByID) Len() int           { return len(a) }
func (a ByID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByID) Less(i, j int) bool { return a[i].ID < a[j].ID }
