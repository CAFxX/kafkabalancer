package main

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"testing"
)

type testCase struct {
	pl  []Partition
	ppl []Partition
	err string
	cfg *RebalanceConfig
}

func wrap(p []Partition) *PartitionList {
	return &PartitionList{
		Version:    1,
		Partitions: p,
	}
}

func TestBalancing(t *testing.T) {
	cfgLeader := DefaultRebalanceConfig()
	cfgLeader.AllowLeaderRebalancing = true

	cfg3Replicas := DefaultRebalanceConfig()
	cfg3Replicas.MinReplicasForRebalancing = 3

	cfg6Brokers := DefaultRebalanceConfig()
	cfg6Brokers.Brokers = []BrokerID{1, 2, 3, 4, 5, 6}

	tc := []testCase{
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{1, 3, 2}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 4, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{4, 2, 3}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4, 5}},
			},
			cfg: &cfgLeader,
		},

		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 1, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3, 4}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4, 5}},
			},
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 3}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4, 5}},
			},
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3, 4}, Weight: 1.0},
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 2, 5}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 3, Replicas: []BrokerID{1, 3, 5}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4, 5}},
			},
		},

		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 3}, Weight: 1.0},
				Partition{Topic: "b", Partition: 1, Replicas: []BrokerID{4, 3, 2}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "b", Partition: 1, Replicas: []BrokerID{4, 3, 1}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4}},
			},
			cfg: &cfg3Replicas,
		},

		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 3}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4, 5, 6}},
			},
			cfg: &cfg6Brokers,
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 3}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 5}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3, 4, 5, 6}},
			},
			cfg: &cfg6Brokers,
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 4, 5}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0},
			},
			cfg: &cfg6Brokers,
		},

		// remove extra replica
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0, NumReplicas: 2},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 3}, Weight: 1.0, NumReplicas: 2, Brokers: []BrokerID{1, 2, 3}},
			},
		},

		// add missing replica
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3}},
			},
			ppl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2, 3}, Weight: 1.0, NumReplicas: 3, Brokers: []BrokerID{1, 2, 3}},
			},
		},

		// duplicate replicas
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 1}, Weight: 1.0, Brokers: []BrokerID{1, 2}},
			},
			err: "has duplicated replicas",
		},

		// all weights missing
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 1}},
			},
		},

		// one weight missing
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 1}},
			},
			err: "has no weight",
		},
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 1}, Weight: 1.0},
			},
			err: "has no weight",
		},

		// negative weight
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}, Weight: 1.0},
				Partition{Topic: "a", Partition: 2, Replicas: []BrokerID{2, 1}, Weight: -1.0},
			},
			err: "has negative weight",
		},

		// unable to add replica
		testCase{
			pl: []Partition{
				Partition{Topic: "a", Partition: 1, Replicas: []BrokerID{1, 2}, NumReplicas: 3},
			},
			err: "unable to pick replica to add",
		},
	}

	for _, c := range tc {
		log.SetOutput(&bytes.Buffer{})
		pl := wrap(c.pl)

		cfg := DefaultRebalanceConfig()
		if c.cfg != nil {
			cfg = *c.cfg
		}

		ppl, err := Balance(pl, cfg)

		if c.err != "" {
			if !strings.Contains(err.Error(), c.err) {
				t.Errorf("expected error %v, got %v", c.err, err)
			}
			if ppl != nil {
				t.Errorf("expected nil ppl, got %v", ppl)
			}
		} else if err != nil {
			t.Errorf("unexpected error %v", err)
		} else if !reflect.DeepEqual(wrap(c.ppl), ppl) {
			t.Errorf("expected %v, got %v", wrap(c.ppl), ppl)
			t.Logf("pl %v", c.pl)
		}
	}
}
