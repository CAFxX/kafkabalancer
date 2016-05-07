package main

import (
	"reflect"
	"testing"
)

type testCase struct {
	pl  []Partition
	ppl []Partition
	err error
	cfg *RebalanceConfig
}

func wrap(p []Partition) *PartitionList {
	return &PartitionList{
		Version:    1,
		Partitions: p,
	}
}

func TestBalancing(t *testing.T) {
	cfgNoLeader := DefaultRebalanceConfig()
	cfgNoLeader.AllowLeaderRebalancing = false

	cfg3Replicas := DefaultRebalanceConfig()
	cfg3Replicas.MinReplicasForRebalancing = 3

	tc := []testCase{
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{1, 2, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{4, 2, 3}},
			},
		},

		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 1, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3, 4}},
			},
			cfg: &cfgNoLeader,
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 5, 3}},
			},
			cfg: &cfgNoLeader,
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			cfg: &cfgNoLeader,
		},

		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3}, Weight: 1.0},
				Partition{Topic: "b", Partition: 1, Replicas: []BrokerID{2, 3, 4}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "b", Partition: 1, Replicas: []BrokerID{1, 3, 4}},
			},
			cfg: &cfg3Replicas,
		},
	}

	for _, c := range tc {
		pl := wrap(c.pl)

		cfg := DefaultRebalanceConfig()
		if c.cfg != nil {
			cfg = *c.cfg
		}

		ppl, err := AnalyzeDistribution(pl, cfg)

		if !reflect.DeepEqual(wrap(c.ppl), ppl) {
			t.Errorf("expected %v, got %v", wrap(c.ppl), ppl)
		}
		if c.err != nil && err == nil || c.err == nil && err != nil {
			t.Errorf("expected error %v, got %v", c.err, err)
		}
	}
}
