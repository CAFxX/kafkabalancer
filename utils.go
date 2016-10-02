package main

import (
	"fmt"
	"sort"
)

type byBrokerID []BrokerID

func (a byBrokerID) Len() int           { return len(a) }
func (a byBrokerID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byBrokerID) Less(i, j int) bool { return a[i] < a[j] }

type brokerLoad struct {
	ID   BrokerID
	Load float64
}

type byBrokerLoad []brokerLoad

func (a byBrokerLoad) Len() int      { return len(a) }
func (a byBrokerLoad) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byBrokerLoad) Less(i, j int) bool {
	if a[i].Load != a[j].Load {
		return a[i].Load < a[j].Load
	}
	return a[i].ID < a[j].ID
}

func toBrokerSet(brokers []BrokerID) map[BrokerID]struct{} {
	b := make(map[BrokerID]struct{})
	for _, id := range brokers {
		b[id] = struct{}{}
	}

	return b
}

func inBrokerList(haystack []BrokerID, needle BrokerID) bool {
	for _, b := range haystack {
		if b == needle {
			return true
		}
	}

	return false
}

func getBrokerList(pl *PartitionList) []BrokerID {
	b := make(map[BrokerID]struct{})
	for _, p := range pl.Partitions {
		for _, r := range p.Replicas {
			b[r] = struct{}{}
		}
	}

	var brokers []BrokerID
	for id := range b {
		brokers = append(brokers, id)
	}
	sort.Sort(byBrokerID(brokers))

	return brokers
}

func getBrokerListByLoad(loads map[BrokerID]float64, brokers []BrokerID) []BrokerID {
	b := make([]brokerLoad, 0, len(brokers))
	for _, id := range brokers {
		b = append(b, brokerLoad{ID: id, Load: loads[id]})
	}
	sort.Sort(byBrokerLoad(b))

	r := make([]BrokerID, 0, len(brokers))
	for _, broker := range b {
		r = append(r, broker.ID)
	}

	return r
}

func getBrokerListByLoadBL(loads []brokerLoad, brokers []BrokerID) []BrokerID {
	r := make([]BrokerID, 0, len(brokers))
	for _, load := range loads {
		if inBrokerList(brokers, load.ID) {
			r = append(r, load.ID)
		}
	}

	return r
}

func getBrokerLoad(pl *PartitionList) map[BrokerID]float64 {
	b := make(map[BrokerID]float64)
	for _, p := range pl.Partitions {
		for idx, r := range p.Replicas {
			if idx == 0 {
				b[r] += p.Weight * float64(len(p.Replicas)+p.NumConsumers)
			} else {
				b[r] += p.Weight
			}
		}
	}

	return b
}

func getBL(loads map[BrokerID]float64) []brokerLoad {
	// if we don't iterate in a constant order, float arithmetic causes the
	// results to change in the LSBs
	brokers := make([]brokerLoad, 0, len(loads))
	for id, load := range loads {
		brokers = append(brokers, brokerLoad{ID: id, Load: load})
	}
	sort.Sort(byBrokerLoad(brokers))

	return brokers
}

func getUnbalanceBL(brokers []brokerLoad) float64 {
	var sumBrokerLoad float64
	var maxBrokerLoad float64

	for _, broker := range brokers {
		sumBrokerLoad += broker.Load
		if maxBrokerLoad < broker.Load {
			maxBrokerLoad = broker.Load
		}
	}

	avgBrokerLoad := sumBrokerLoad / float64(len(brokers))

	var brokerUnbalance float64
	for _, broker := range brokers {
		relBrokerLoad := broker.Load/avgBrokerLoad - 1.0
		if relBrokerLoad > 0 {
			brokerUnbalance += relBrokerLoad * relBrokerLoad
		} else {
			brokerUnbalance += relBrokerLoad * relBrokerLoad / 2
		}
	}

	return brokerUnbalance
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

func addpl(p Partition, b BrokerID) *PartitionList {
	p.Replicas = append(p.Replicas, b)
	return singlepl(p)
}
