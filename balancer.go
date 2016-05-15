package main

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
)

// RebalanceConfig contains the configuration that drives the rebalancing.
type RebalanceConfig struct {
	AllowLeaderRebalancing    bool
	MinReplicasForRebalancing int
	MinUnbalance              float64

	Brokers []BrokerID
}

// DefaultRebalanceConfig returns the default RebalanceConfig. These values are
// also the one used as the default values for the CLI flags.
func DefaultRebalanceConfig() RebalanceConfig {
	return RebalanceConfig{
		AllowLeaderRebalancing:    false,
		MinReplicasForRebalancing: 2,
		MinUnbalance:              0.00001,
	}
}

var steps = []func(*PartitionList, RebalanceConfig) (*PartitionList, error){
	ValidateWeights,
	ValidateReplicas,
	FillDefaults,
	RemoveExtraReplicas,
	AddMissingReplicas,
	MoveDisallowedReplicas,
	MoveNonLeaders,
	MoveLeaders,
}

// Balance analyzes the workload distribution among brokers for the
// partitions listed in the argument. It returns a PartitionList with 0 or more
// partition reassignments.
func Balance(pl *PartitionList, cfg RebalanceConfig) (*PartitionList, error) {
	log.Printf("rebalance config: %+v", cfg)

	for _, step := range steps {
		stepFunc := runtime.FuncForPC(reflect.ValueOf(step).Pointer())
		stepName := strings.TrimPrefix(stepFunc.Name(), "main.")
		ppl, err := step(pl, cfg)
		if err != nil {
			return nil, fmt.Errorf("%s: %s", stepName, err)
		}
		if ppl != nil {
			log.Printf("%s: %v", stepName, ppl)
			return ppl, nil
		}
	}

	log.Print("no candidate changes")
	return emptypl(), nil
}
