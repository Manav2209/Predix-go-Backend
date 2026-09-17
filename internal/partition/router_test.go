package partition

import (
	"sort"
	"testing"
)

func TestRouterPartitionDeterministic(t *testing.T) {
	r1 := NewRouter(4, "commands")
	r2 := NewRouter(4, "commands")

	events := []string{"event-a", "event-b", "event-c", "event-d", "event-e"}

	for _, e := range events {
		if r1.Partition(e) != r2.Partition(e) {
			t.Errorf("two routers disagree on partition for %s", e)
		}
	}
}

func TestRouterPartitionInBounds(t *testing.T) {
	r := NewRouter(3, "cmds")
	for i := 0; i < 100; i++ {
		p := r.Partition("evt-" + string(rune('a'+i%26)))
		if p < 0 || p >= 3 {
			t.Fatalf("partition %d out of bounds [0,3)", p)
		}
	}
}

func TestRouterPartitionEvenDistribution(t *testing.T) {
	r := NewRouter(4, "commands")
	counts := make(map[int]int)

	for i := 0; i < 1000; i++ {
		key := "event-" + string(rune(i/26+'a')) + string(rune(i%26+'a'))
		counts[r.Partition(key)]++
	}

	for p := 0; p < 4; p++ {
		if counts[p] < 100 {
			t.Errorf("partition %d has only %d events (< 100), likely skewed", p, counts[p])
		}
	}
}

func TestRouterStreamNaming(t *testing.T) {
	r := NewRouter(2, "cmds")

	if got := r.Stream(0); got != "cmds:partition:0" {
		t.Errorf("Stream(0) = %s, want cmds:partition:0", got)
	}

	if got := r.Stream(1); got != "cmds:partition:1" {
		t.Errorf("Stream(1) = %s, want cmds:partition:1", got)
	}

	if got := r.Group(0); got != "engines:p:0" {
		t.Errorf("Group(0) = %s, want engines:p:0", got)
	}

	if got := r.SequenceKey(1); got != "cmds:1:sequence" {
		t.Errorf("SequenceKey(1) = %s, want cmds:1:sequence", got)
	}
}

func TestRouterStreamForEventAgrees(t *testing.T) {
	r := NewRouter(4, "commands")
	for i := 0; i < 50; i++ {
		key := "evt-" + string(rune(i))
		if r.StreamForEvent(key) != r.Stream(r.Partition(key)) {
			t.Errorf("StreamForEvent and Stream(Partition(...)) disagree for %s", key)
		}
	}
}

func TestRouterPartitionStreams(t *testing.T) {
	r := NewRouter(3, "commands")
	streams := r.PartitionStreams()
	if len(streams) != 3 {
		t.Fatalf("PartitionStreams returned %d, want 3", len(streams))
	}

	if !sort.StringsAreSorted(streams) {
		t.Error("PartitionStreams not sorted")
	}
}

func TestRouterDefaults(t *testing.T) {
	r1 := NewRouter(0, "")
	if r1.Partitions != 1 {
		t.Errorf("negative partition count defaulted to %d, want 1", r1.Partitions)
	}

	r2 := NewRouter(2, "")
	if got := r2.Stream(0); got != "commands:partition:0" {
		t.Errorf("default prefix: Stream(0) = %s", got)
	}
}
