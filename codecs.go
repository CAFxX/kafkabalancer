package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

func ParsePartitionList(in io.Reader, isJSON bool) (*PartitionList, error) {
	pl := &PartitionList{}

	if isJSON {
		dec := json.NewDecoder(in)
		err := dec.Decode(pl)
		if err != nil {
			return nil, fmt.Errorf("failed parsing json: %s", err)
		}
		if pl.Version != 1 {
			return nil, fmt.Errorf("wrong partition list version: expected 1, got %d", pl.Version)
		}
	} else {
		scanner := bufio.NewScanner(in)
		re := regexp.MustCompile("^\tTopic: ([^\t]*)\tPartition: ([0-9]*)\tLeader: ([0-9]*)\tReplicas: ([0-9,]*)\tIsr: ([0-9,]*)")
		for scanner.Scan() {
			m := re.FindStringSubmatch(scanner.Text())
			if m == nil {
				continue
			}
			partition, _ := strconv.Atoi(m[2])
			strreplicas := strings.Split(m[4], ",")
			var replicas []BrokerID
			for _, strreplica := range strreplicas {
				replica, _ := strconv.Atoi(strreplica)
				replicas = append(replicas, BrokerID(replica))
			}
			pl.Partitions = append(pl.Partitions, Partition{
				Topic:     TopicName(m[1]),
				Partition: PartitionID(partition),
				Replicas:  replicas,
			})
		}

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("failed reading file: %s", err)
		}
	}

	if len(pl.Partitions) == 0 {
		return nil, fmt.Errorf("empty partition list")
	}

	weightless := pl.Partitions[0].Weight == 0
	for idx, p := range pl.Partitions {
		if p.Weight == 0 && !weightless {
			return nil, fmt.Errorf("topic %s partition %d has no weight but other topics have it", p.Topic, p.Partition)
		} else if p.Weight != 0 && weightless {
			return nil, fmt.Errorf("topic %s partition %d has a weight but other topics don't have it", p.Topic, p.Partition)
		} else if p.Weight < 0 {
			return nil, fmt.Errorf("topic %s partition %d has a negative weight", p.Topic, p.Partition)
		} else if p.Weight == 0 && weightless {
			pl.Partitions[idx].Weight = 1
		}
	}

	return pl, nil
}

func WritePartitionList(out io.Writer, pl *PartitionList) error {
	enc := json.NewEncoder(out)
	err := enc.Encode(pl)
	if err != nil {
		return fmt.Errorf("failed serializing json: %s", err)
	}

	return nil
}
