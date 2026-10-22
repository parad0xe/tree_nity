package topic

import (
	"fmt"
	"maps"
	"strings"
	"testing"

	id "tree_nity/src/server/utils"
	"tree_nity/src/server/utils/trie"
)

func countConsumers(consumers []id.ID) map[id.ID]int {
	counts := make(map[id.ID]int)
	for _, subscriber := range consumers {
		counts[subscriber]++
	}
	return counts
}

func newTestConsumerTrie(t *testing.T, entries map[string][]id.ID) *ConsumerTrie {
	t.Helper()
	trie := NewConsumerTrie()
	for prefix, subscribers := range entries {
		for _, subscriber := range subscribers {
			if err := trie.Subscribe(prefix, subscriber); err != nil {
				t.Errorf("Subscribe(%q, %q) during setup error = %v, want nil", prefix, subscriber, err)
				return nil
			}
		}
	}
	return trie
}

// TestNewConsumerTrie checks construction and searching an empty index.
func TestNewConsumerTrie(t *testing.T) {
	trie := NewConsumerTrie()
	if trie == nil || trie.trie == nil {
		t.Errorf("NewConsumerTrie() = %#v, want an initialized consumer trie", trie)
		return
	}
	got, err := trie.Search("")
	if err != nil || len(got) != 0 {
		t.Errorf("Search(%q) on a new consumer trie = %v, error = %v, want no consumers and nil error", "", got, err)
	}
}

// TestConsumerTrieUninitialized checks wrapper validation and underlying trie errors.
func TestConsumerTrieUninitialized(t *testing.T) {
	tests := []struct {
		name    string
		trie    *ConsumerTrie
		wantErr string
	}{
		{name: "nil receiver", wantErr: "consumer trie is not initialized"},
		{name: "zero value", trie: &ConsumerTrie{}, wantErr: "consumer trie is not initialized"},
		{name: "uninitialized inner trie", trie: &ConsumerTrie{trie: &trie.Trie[id.ID]{}}, wantErr: "trie is not initialized"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber := id.ID("subscriber")
			operations := []struct {
				name string
				run  func() ([]id.ID, error)
			}{
				{name: "Subscribe", run: func() ([]id.ID, error) {
					return nil, tt.trie.Subscribe("a", subscriber)
				}},
				{name: "Unsubscribe", run: func() ([]id.ID, error) {
					return nil, tt.trie.Unsubscribe("a", subscriber)
				}},
				{name: "Search", run: func() ([]id.ID, error) {
					return tt.trie.Search("a")
				}},
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					got, err := operation.run()
					if err == nil || err.Error() != tt.wantErr {
						t.Errorf("%s with prefix %q and fixture consumer %q on %s error = %v, want %q", operation.name, "a", subscriber, tt.name, err, tt.wantErr)
					}
					if got != nil {
						t.Errorf("%s with prefix %q on %s returned %v, want nil", operation.name, "a", tt.name, got)
					}
				})
			}
		})
	}
}

// TestConsumerTrieEmptyID checks rejection without changing existing subscriptions.
func TestConsumerTrieEmptyID(t *testing.T) {
	operations := []struct {
		name string
		run  func(*ConsumerTrie, string, id.ID) error
	}{
		{name: "Subscribe", run: (*ConsumerTrie).Subscribe},
		{name: "Unsubscribe", run: (*ConsumerTrie).Unsubscribe},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			subscriber := id.ID("subscriber")
			trie := newTestConsumerTrie(t, map[string][]id.ID{"a": {subscriber}})
			if trie == nil {
				return
			}
			if err := operation.run(trie, "a", ""); err == nil || err.Error() != "consumer ID is empty" {
				t.Errorf("%s(%q, \"\") error = %v, want %q", operation.name, "a", err, "consumer ID is empty")
			}
			got, err := trie.Search("a")
			if err != nil || !maps.Equal(countConsumers(got), countConsumers([]id.ID{subscriber})) {
				t.Errorf("Search(%q) after %s(%q, \"\") = %v, error = %v, want [%q] and nil error", "a", operation.name, "a", got, err, subscriber)
			}
		})
	}
}

// TestConsumerTrieSubscribe checks identity by ID and duplicate rejection per prefix.
func TestConsumerTrieSubscribe(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
	}{
		{name: "root"},
		{name: "single byte", prefix: "a"},
		{name: "UTF-8", prefix: "é/猫"},
		{name: "binary bytes", prefix: "\x00\xff"},
		{name: "long prefix", prefix: strings.Repeat("a", 4096)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := NewConsumerTrie()
			first, second := id.ID("first"), id.ID("second")
			if err := trie.Subscribe(tt.prefix, first); err != nil {
				t.Errorf("Subscribe(%q, %q) error = %v, want nil", tt.prefix, first, err)
				return
			}
			if err := trie.Subscribe(tt.prefix, first); err == nil || err.Error() != "value already exists" {
				t.Errorf("Subscribe(%q, %q) a second time error = %v, want %q", tt.prefix, first, err, "value already exists")
			}
			if err := trie.Subscribe(tt.prefix, second); err != nil {
				t.Errorf("Subscribe(%q, %q) after subscribing %q error = %v, want nil", tt.prefix, second, first, err)
				return
			}
			if err := trie.Subscribe(tt.prefix+"/child", first); err != nil {
				t.Errorf("Subscribe(%q, %q) after subscribing at %q error = %v, want nil", tt.prefix+"/child", first, tt.prefix, err)
				return
			}
			key := tt.prefix + "/child/item"
			want := []id.ID{first, second}
			got, err := trie.Search(key)
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(want)) {
				t.Errorf("Search(%q) = %v, error = %v, want distinct consumers %v and nil error", key, got, err, want)
			}
		})
	}
}

// TestConsumerTrieSearch checks matching ancestor prefixes and consumer deduplication.
func TestConsumerTrieSearch(t *testing.T) {
	first, second, third, other := id.ID("first"), id.ID("second"), id.ID("third"), id.ID("other")
	tests := []struct {
		name    string
		entries map[string][]id.ID
		key     string
		want    []id.ID
	}{
		{name: "empty trie", key: "abc"},
		{name: "exact match", entries: map[string][]id.ID{"a": {first}}, key: "a", want: []id.ID{first}},
		{name: "root matches unknown path", entries: map[string][]id.ID{"": {first}}, key: "unknown", want: []id.ID{first}},
		{name: "empty key", entries: map[string][]id.ID{"": {first}, "a": {second}}, want: []id.ID{first}},
		{
			name: "nested prefixes", entries: map[string][]id.ID{"a": {first}, "ab": {second}, "abc": {third}, "x": {other}},
			key: "abcd", want: []id.ID{first, second, third},
		},
		{name: "key shorter than subscription", entries: map[string][]id.ID{"abc": {first}}, key: "ab"},
		{name: "descendant excluded", entries: map[string][]id.ID{"a": {first}, "ab": {second}}, key: "a", want: []id.ID{first}},
		{name: "missing suffix", entries: map[string][]id.ID{"a": {first}, "abcd": {second}}, key: "abx", want: []id.ID{first}},
		{name: "unrelated key", entries: map[string][]id.ID{"a": {first}}, key: "b"},
		{
			name: "deduplicate across prefixes", entries: map[string][]id.ID{"": {first}, "a": {first, second}, "ab": {first, third}},
			key: "abc", want: []id.ID{first, second, third},
		},
		{
			name: "UTF-8", entries: map[string][]id.ID{"é": {first}, "é/猫": {second}, "è": {other}},
			key: "é/猫/item", want: []id.ID{first, second},
		},
		{name: "binary bytes", entries: map[string][]id.ID{"\x00": {first}, "\x00\xff": {second}}, key: "\x00\xff/item", want: []id.ID{first, second}},
		{name: "case sensitive", entries: map[string][]id.ID{"Ab": {first}}, key: "ab"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestConsumerTrie(t, tt.entries)
			if trie == nil {
				return
			}
			got, err := trie.Search(tt.key)
			if err != nil {
				t.Errorf("Search(%q) with subscriptions %v error = %v, want nil", tt.key, tt.entries, err)
			}
			if !maps.Equal(countConsumers(got), countConsumers(tt.want)) {
				t.Errorf("Search(%q) with subscriptions %v = %v, want consumers %v once each", tt.key, tt.entries, got, tt.want)
			}
		})
	}
}

// TestConsumerTrieUnsubscribe checks removal scope and missing subscriptions.
func TestConsumerTrieUnsubscribe(t *testing.T) {
	first, second := id.ID("first"), id.ID("second")
	tests := []struct {
		name       string
		entries    map[string][]id.ID
		prefix     string
		subscriber id.ID
		key        string
		want       []id.ID
		wantErr    string
	}{
		{name: "only subscription", entries: map[string][]id.ID{"a": {first}}, prefix: "a", subscriber: first, key: "a"},
		{name: "other consumer remains", entries: map[string][]id.ID{"a": {first, second}}, prefix: "a", subscriber: first, key: "a", want: []id.ID{second}},
		{name: "descendant remains", entries: map[string][]id.ID{"a": {first}, "ab": {second}}, prefix: "a", subscriber: first, key: "abc", want: []id.ID{second}},
		{name: "ancestor remains", entries: map[string][]id.ID{"a": {first}, "ab": {second}}, prefix: "ab", subscriber: second, key: "abc", want: []id.ID{first}},
		{name: "same consumer at another prefix", entries: map[string][]id.ID{"a": {first}, "ab": {first}}, prefix: "a", subscriber: first, key: "abc", want: []id.ID{first}},
		{name: "root subscription", entries: map[string][]id.ID{"": {first}, "a": {second}}, subscriber: first, key: "a", want: []id.ID{second}},
		{name: "root only", entries: map[string][]id.ID{"": {first}}, subscriber: first},
		{
			name: "missing prefix", entries: map[string][]id.ID{"a": {first}}, prefix: "missing", subscriber: first,
			key: "a", want: []id.ID{first}, wantErr: `prefix "missing" not found in trie`,
		},
		{
			name: "missing consumer", entries: map[string][]id.ID{"a": {first}}, prefix: "a", subscriber: second,
			key: "a", want: []id.ID{first}, wantErr: "value not found in trie",
		},
		{name: "empty trie", prefix: "a", subscriber: first, key: "a", wantErr: `prefix "a" not found in trie`},
		{name: "UTF-8", entries: map[string][]id.ID{"é": {first}, "é/猫": {second}}, prefix: "é", subscriber: first, key: "é/猫", want: []id.ID{second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestConsumerTrie(t, tt.entries)
			if trie == nil {
				return
			}
			err := trie.Unsubscribe(tt.prefix, tt.subscriber)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Errorf("Unsubscribe(%q, %q) with subscriptions %v error = %v, want %q", tt.prefix, tt.subscriber, tt.entries, err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("Unsubscribe(%q, %q) with subscriptions %v error = %v, want nil", tt.prefix, tt.subscriber, tt.entries, err)
			}
			got, err := trie.Search(tt.key)
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(tt.want)) {
				t.Errorf("Search(%q) after Unsubscribe(%q, %q) = %v, error = %v, want consumers %v and nil error", tt.key, tt.prefix, tt.subscriber, got, err, tt.want)
			}
		})
	}
}

// TestConsumerTrieManyConsumers checks deduplication and removal at increasing sizes.
func TestConsumerTrieManyConsumers(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "minimal", count: 1},
		{name: "large", count: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := NewConsumerTrie()
			want := make([]id.ID, 0, tt.count)
			for i := 0; i < tt.count; i++ {
				subscriber := id.ID(fmt.Sprintf("consumer-%d", i))
				want = append(want, subscriber)
				for _, prefix := range []string{"topic", "topic/events"} {
					if err := trie.Subscribe(prefix, subscriber); err != nil {
						t.Errorf("Subscribe(%q, %q) at index %d of %d error = %v, want nil", prefix, subscriber, i, tt.count, err)
						return
					}
				}
			}
			got, err := trie.Search("topic/events/item")
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(want)) {
				t.Errorf("Search(%q) with %d consumers at two prefixes = %v, error = %v, want consumers %v once each and nil error", "topic/events/item", tt.count, got, err, want)
			}
			for _, subscriber := range want {
				if err := trie.Unsubscribe("topic", subscriber); err != nil {
					t.Errorf("Unsubscribe(%q, %q) with %d consumers error = %v, want nil", "topic", subscriber, tt.count, err)
					return
				}
			}
			got, err = trie.Search("topic/events/item")
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(want)) {
				t.Errorf("Search(%q) after removing %d parent subscriptions = %v, error = %v, want consumers %v and nil error", "topic/events/item", tt.count, got, err, want)
			}
			for _, subscriber := range want {
				if err := trie.Unsubscribe("topic/events", subscriber); err != nil {
					t.Errorf("Unsubscribe(%q, %q) with %d consumers error = %v, want nil", "topic/events", subscriber, tt.count, err)
					return
				}
			}
			if got, err := trie.Search("topic/events/item"); err != nil || len(got) != 0 {
				t.Errorf("Search(%q) after removing all %d consumers = %v, error = %v, want no consumers and nil error", "topic/events/item", tt.count, got, err)
			}
		})
	}
}
