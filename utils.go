package main

import "sort"

type byBrokerID []BrokerID

func (a byBrokerID) Len() int           { return len(a) }
func (a byBrokerID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byBrokerID) Less(i, j int) bool { return a[i] < a[j] }

type byBrokerLoad []BrokerLoad

func (a byBrokerLoad) Len() int           { return len(a) }
func (a byBrokerLoad) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byBrokerLoad) Less(i, j int) bool { return a[i].Load < a[j].Load }

func toBrokerSet(brokers []BrokerID) map[BrokerID]struct{} {
	b := make(map[BrokerID]struct{})
	for _, id := range brokers {
		b[id] = struct{}{}
	}

	return b
}

func toBrokerList(brokers map[BrokerID]struct{}) []BrokerID {
	b := make([]BrokerID, 0, len(brokers))
	for id := range brokers {
		b = append(b, id)
	}

	return b
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
	b := make([]BrokerLoad, 0, len(brokers))
	for _, id := range brokers {
		b = append(b, BrokerLoad{ID: id, Load: loads[id]})
	}
	sort.Sort(byBrokerLoad(b))

	r := make([]BrokerID, 0, len(brokers))
	for _, broker := range b {
		r = append(r, broker.ID)
	}

	return r
}

func getBrokerLoad(pl *PartitionList) map[BrokerID]float64 {
	b := make(map[BrokerID]float64)
	for _, p := range pl.Partitions {
		for _, r := range p.Replicas {
			b[r] += p.Weight
		}
	}

	return b
}

func getUnbalance(loads map[BrokerID]float64) float64 {
	var sumBrokerLoad float64
	var maxBrokerLoad float64

	for _, load := range loads {
		sumBrokerLoad += load
		if maxBrokerLoad < load {
			maxBrokerLoad = load
		}
	}

	avgBrokerLoad := sumBrokerLoad / float64(len(loads))

	var brokerUnbalance float64
	for _, load := range loads {
		relBrokerLoad := load/avgBrokerLoad - 1.0
		if relBrokerLoad > 0 {
			brokerUnbalance += relBrokerLoad * relBrokerLoad
		} else {
			brokerUnbalance += relBrokerLoad * relBrokerLoad / 2
		}
	}

	return brokerUnbalance
}
