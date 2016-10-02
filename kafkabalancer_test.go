package main

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"
)

func TestMainHelp(t *testing.T) {
	json, _ := ioutil.ReadFile("test/test.json")
	in, out, err := bytes.NewBuffer(json), &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(in, out, err, []string{"kafkabalancer", "-help"})
	if rv != 0 {
		t.Fatalf("unexpected rv %d", rv)
	}
	if !strings.Contains(err.String(), "Usage of kafkabalancer:") {
		t.Fatalf("missing expected string: %s", err.String())
	}
}

func TestMainStdin(t *testing.T) {
	json, _ := ioutil.ReadFile("test/test.json")
	in, out, err := bytes.NewBuffer(json), &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(in, out, err, []string{"kafkabalancer", "-input-json"})
	if rv != 0 {
		t.Fatalf("unexpected rv %d", rv)
	}
}

func TestMainFile(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json"})
	if rv != 0 {
		t.Fatalf("unexpected rv %d", rv)
	}
}

func TestMainFileAndZk(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json", "-from-zk=localhost:2282"})
	if rv != 3 {
		t.Fatalf("unexpected rv %d", rv)
	}
	if !strings.Contains(err.String(), "can't specify both -input and -from-zk") {
		t.Fatalf("missing expected string: %s", err.String())
	}
}

func TestMainPartitionListEmpty(t *testing.T) {
	in, out, err := bytes.NewBufferString(""), &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(in, out, err, []string{"kafkabalancer", "-input-json"})
	if rv != 2 {
		t.Fatalf("unexpected rv %d", rv)
	}
	if !strings.Contains(err.String(), "failed getting partition list:") {
		t.Fatalf("missing expected string: %s", err.String())
	}
}

func TestMainPartitionListMalformed(t *testing.T) {
	in, out, err := bytes.NewBufferString("::malformed::"), &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(in, out, err, []string{"kafkabalancer", "-input-json"})
	if rv != 2 {
		t.Fatalf("unexpected rv %d", rv)
	}
	if !strings.Contains(err.String(), "failed getting partition list:") {
		t.Fatalf("missing expected string: %s", err.String())
	}
}

func TestMainFileMissing(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/missing.json"})
	if rv != 1 {
		t.Fatalf("unexpected rv %d", rv)
	}
}

func TestMainBrokerList(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json", "-broker-ids=1,2,3"})
	if rv != 0 {
		t.Fatalf("unexpected rv %d", rv)
	}
}

func TestMainBrokerListMalformed(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json", "-broker-ids=malformed"})
	if rv != 3 {
		t.Fatalf("unexpected rv %d", rv)
	}
	if !strings.Contains(err.String(), "failed parsing broker list") {
		t.Fatalf("missing expected string: %s", err.String())
	}
}

func TestMainMaxReassignMalformed(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json", "-max-reassign=-1"})
	if rv != 3 {
		t.Fatalf("unexpected rv %d", rv)
	}
	if !strings.Contains(err.String(), "invalid number of max reassignments") {
		t.Fatalf("missing expected string: %s", err.String())
	}
}

func TestMainMaxReassignHuge(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json", "-max-reassign=1000"})
	if rv != 0 {
		t.Fatalf("unexpected rv %d", rv)
	}
}

func TestMainFullOutput(t *testing.T) {
	out, err := &bytes.Buffer{}, &bytes.Buffer{}
	rv := run(nil, out, err, []string{"kafkabalancer", "-input-json", "-input=test/test.json", "-full-output"})
	if rv != 0 {
		t.Fatalf("unexpected rv %d", rv)
	}
}
