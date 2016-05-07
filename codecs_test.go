package main

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestParsingJSON(t *testing.T) {
	const jsonStr = `{"version":1,
   "partitions":[{"topic":"foo1","partition":2,"replicas":[1,2]},
                 {"topic":"foo1","partition":0,"replicas":[1,2]},
                 {"topic":"foo2","partition":2,"replicas":[1,2]},
                 {"topic":"foo2","partition":0,"replicas":[1,3]},
                 {"topic":"foo1","partition":1,"replicas":[1,3]},
                 {"topic":"foo2","partition":1,"replicas":[1,4]}]
  }`

	_, err := ParsePartitionList(bytes.NewBufferString(jsonStr), true)

	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestWritingJSON(t *testing.T) {
	const jsonStr = `{"version":1,
   "partitions":[{"topic":"foo1","partition":2,"replicas":[1,2]},
                 {"topic":"foo1","partition":0,"replicas":[1,2]},
                 {"topic":"foo2","partition":2,"replicas":[1,2]},
                 {"topic":"foo2","partition":0,"replicas":[1,3]},
                 {"topic":"foo1","partition":1,"replicas":[1,3]},
                 {"topic":"foo2","partition":1,"replicas":[1,4]}]
  }`

	pl, err := ParsePartitionList(bytes.NewBufferString(jsonStr), true)

	if err != nil {
		t.Skip()
	}

	WritePartitionList(ioutil.Discard, pl)
}

func TestParsingText(t *testing.T) {
	const textStr = `Topic:test	PartitionCount:9	ReplicationFactor:3	Configs:
	Topic: test	Partition: 0	Leader: 2	Replicas: 2,0,1	Isr: 0,1,2
	Topic: test	Partition: 1	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Topic: test	Partition: 2	Leader: 1	Replicas: 1,2,0	Isr: 0,1,2
	Topic: test	Partition: 3	Leader: 2	Replicas: 2,1,0	Isr: 0,1,2
	Topic: test	Partition: 4	Leader: 0	Replicas: 0,2,1	Isr: 0,1,2
	Topic: test	Partition: 5	Leader: 1	Replicas: 1,0,2	Isr: 0,1,2
	Topic: test	Partition: 6	Leader: 2	Replicas: 2,0,1	Isr: 0,1,2
	Topic: test	Partition: 7	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2
	Topic: test	Partition: 8	Leader: 1	Replicas: 1,2,0	Isr: 0,1,2`

	_, err := ParsePartitionList(bytes.NewBufferString(textStr), false)

	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}
