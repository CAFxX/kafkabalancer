package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/cafxx/kafkabalancer/logbuf"
	"github.com/pkg/profile"
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

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr, os.Args))
}

func run(i io.Reader, o io.Writer, e io.Writer, args []string) int {
	be := logbuf.NewDefaultBufferingWriter(e)
	defer be.Close()
	log.SetOutput(be)

	f := flag.NewFlagSet("kafkabalancer", flag.ContinueOnError)
	f.SetOutput(be)
	jsonInput := f.Bool("input-json", false, "Parse the input as JSON")
	input := f.String("input", "", "Name of the file to read (if no file is specified read from stdin, can not be used with -from-zk)")
	fromZK := f.String("from-zk", "", "Zookeeper connection string (can not be used with -input)")
	maxReassign := f.Int("max-reassign", 1, "Maximum number of reassignments to generate")
	fullOutput := f.Bool("full-output", false, "Output the full partition list: by default only the changes are printed")
	pprof := f.Bool("pprof", false, "Enable CPU profiling")
	allowLeader := f.Bool("allow-leader", DefaultRebalanceConfig().AllowLeaderRebalancing, "Consider the partition leader eligible for rebalancing")
	minReplicas := f.Int("min-replicas", DefaultRebalanceConfig().MinReplicasForRebalancing, "Minimum number of replicas for a partition to be eligible for rebalancing")
	minUnbalance := f.Float64("min-umbalance", DefaultRebalanceConfig().MinUnbalance, "Minimum umbalance value required to perform rebalancing")
	brokerIDs := f.String("broker-ids", "auto", "Comma-separated list of broker IDs")
	help := f.Bool("help", false, "Display usage")
	f.Usage = func() {
		fmt.Fprintf(be, "Usage of %s:\n", args[0])
		f.PrintDefaults()
	}
	f.Parse(args[1:])

	if *pprof {
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	}

	if *help {
		f.Usage()
		return 0
	}

	var brokers []BrokerID
	if *brokerIDs != "auto" {
		for _, broker := range strings.Split(*brokerIDs, ",") {
			b, cerr := strconv.Atoi(broker)
			if cerr != nil {
				log.Printf("failed parsing broker list \"%s\": %s", *brokerIDs, cerr)
				f.Usage()
				return 3
			}
			brokers = append(brokers, BrokerID(b))
		}
	}

	if *maxReassign < 0 {
		log.Printf("invalid number of max reassignments \"%d\"", *maxReassign)
		f.Usage()
		return 3
	}

	if *input != "" && *fromZK != "" {
		log.Print("can't specify both -input and -from-zk")
		f.Usage()
		return 3
	}

	var err error

	in := i
	if *input != "" {
		in, err = os.Open(*input)
		if err != nil {
			log.Printf("failed opening file %s: %s", *input, err)
			return 1
		}
		defer in.(io.Closer).Close()
	}

	out := o

	var pl *PartitionList
	if *fromZK != "" {
		pl, err = GetPartitionListFromZookeeper(*fromZK)
	} else {
		pl, err = GetPartitionListFromReader(in, *jsonInput)
	}
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

	be.Flush(true)

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
