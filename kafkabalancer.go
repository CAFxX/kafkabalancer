package main

import (
	"flag"
	"log"
	"os"
	"io"
	"strconv"
	"strings"

	"github.com/pkg/profile"
	"github.com/cafxx/kafkabalancer/logbuf"
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
	Weight       float64    `json:"weight,omitempty"`        // default: 1.0
	NumReplicas  int        `json:"num_replicas,omitempty"`  // default: len(replicas)
	Brokers      []BrokerID `json:"brokers,omitempty"`       // default: (auto)
	NumConsumers int        `json:"num_consumers,omitempty"` // default: 1
}

var jsonInput = flag.Bool("input-json", false, "Parse the input as JSON")
var input = flag.String("input", "", "Name of the file to read (if no file is specified, read from stdin)")
var maxReassign = flag.Int("max-reassign", 1, "Maximum number of reassignments to generate")
var fullOutput = flag.Bool("full-output", false, "Output the full")
var pprof = flag.Bool("pprof", false, "Enable CPU profiling")

var allowLeader = flag.Bool("allow-leader", DefaultRebalanceConfig().AllowLeaderRebalancing, "Consider the partition leader eligible for rebalancing")
var minReplicas = flag.Int("min-replicas", DefaultRebalanceConfig().MinReplicasForRebalancing, "Minimum number of replicas for a partition to be eligible for rebalancing")
var minUnbalance = flag.Float64("min-umbalance", DefaultRebalanceConfig().MinUnbalance, "Minimum umbalance value required to perform rebalancing")

var brokerIDs = flag.String("broker-ids", "auto", "Comma-separated list of broker IDs")

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr, os.Args))
}

func run(i io.Reader, o io.Writer, e io.Writer, args []string) int {
	flag.CommandLine.Parse(args[1:])
	var err error

	if *pprof {
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	}

	be := logbuf.NewDefaultBufferingWriter(e)
	defer be.Close()
	log.SetOutput(be)

	var brokers []BrokerID
	if *brokerIDs != "auto" {
		for _, broker := range strings.Split(*brokerIDs, ",") {
			b, cerr := strconv.Atoi(broker)
			if cerr != nil {
				log.Printf("failed parsing broker list \"%s\": %s", *brokerIDs, err)
				flag.Usage()
				return 3
			}
			brokers = append(brokers, BrokerID(b))
		}
	}

	if *maxReassign < 0 {
		log.Printf("invalid number of max reassignments \"%d\"", *maxReassign)
		flag.Usage()
		return 3
	}

	in := i
	if *input != "" {
		in, err = os.Open(*input)
		if err != nil {
			log.Printf("failed opening file %s: %s", *input, err)
			return 1
		}
		if inc, ok := in.(io.Closer); ok {
			defer inc.Close()
		}
	}

	out := o

	pl, err := ParsePartitionList(in, *jsonInput)
	if err != nil {
		log.Printf("failed parsing partition list: %s", err)
		return 2
	}

	cfg := RebalanceConfig{
		AllowLeaderRebalancing:    *allowLeader,
		MinReplicasForRebalancing: *minReplicas,
		MinUnbalance:              *minUnbalance,
		Brokers:                   brokers,
	}

	log.Printf("rebalance config: %+v", cfg)

	opl := emptypl()

	for i := 0; i < *maxReassign; i++ {
		ppl, err := Balance(pl, cfg)
		if err != nil {
			log.Printf("failed optimizing distribution: %s", err)
			return 3
		}

		if len(ppl.Partitions) == 0 {
			break
		}

		opl.Partitions = append(opl.Partitions, ppl.Partitions...)
	}

	if *fullOutput {
		opl = pl
	}
	err = WritePartitionList(out, opl)
	if err != nil {
		log.Printf("failed writing partition list: %s", err)
		return 4
	}

	return 0
}
