package topic

import (
	"maps"
	"strings"
	"testing"

	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/trie"
)

func countConsumers(consumers []*consumer.Consumer) map[*consumer.Consumer]int {
	counts := make(map[*consumer.Consumer]int)
	for _, subscriber := range consumers {
		counts[subscriber]++
	}
	return counts
}

func newTestConsumerTrie(t *testing.T, entries map[string][]*consumer.Consumer) *ConsumerTrie {
	t.Helper()
	trie := NewConsumerTrie()
	for prefix, subscribers := range entries {
		for _, subscriber := range subscribers {
			if err := trie.Subscribe(prefix, subscriber); err != nil {
				t.Errorf("Subscribe(%q, %p) during setup error = %v, want nil", prefix, subscriber, err)
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
		{name: "uninitialized inner trie", trie: &ConsumerTrie{trie: &trie.Trie[*consumer.Consumer]{}}, wantErr: "trie is not initialized"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriber := &consumer.Consumer{}
			operations := []struct {
				name string
				run  func() ([]*consumer.Consumer, error)
			}{
				{name: "Subscribe", run: func() ([]*consumer.Consumer, error) {
					return nil, tt.trie.Subscribe("a", subscriber)
				}},
				{name: "Unsubscribe", run: func() ([]*consumer.Consumer, error) {
					return nil, tt.trie.Unsubscribe("a", subscriber)
				}},
				{name: "Search", run: func() ([]*consumer.Consumer, error) {
					return tt.trie.Search("a")
				}},
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					got, err := operation.run()
					if err == nil || err.Error() != tt.wantErr {
						t.Errorf("%s with prefix %q and fixture consumer %p on %s error = %v, want %q", operation.name, "a", subscriber, tt.name, err, tt.wantErr)
					}
					if got != nil {
						t.Errorf("%s with prefix %q on %s returned %v, want nil", operation.name, "a", tt.name, got)
					}
				})
			}
		})
	}
}

// TestConsumerTrieNilConsumer checks rejection without changing existing subscriptions.
func TestConsumerTrieNilConsumer(t *testing.T) {
	operations := []struct {
		name string
		run  func(*ConsumerTrie, string, *consumer.Consumer) error
	}{
		{name: "Subscribe", run: (*ConsumerTrie).Subscribe},
		{name: "Unsubscribe", run: (*ConsumerTrie).Unsubscribe},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			subscriber := &consumer.Consumer{}
			trie := newTestConsumerTrie(t, map[string][]*consumer.Consumer{"a": {subscriber}})
			if trie == nil {
				return
			}
			if err := operation.run(trie, "a", nil); err == nil || err.Error() != "consumer is nil" {
				t.Errorf("%s(%q, nil) error = %v, want %q", operation.name, "a", err, "consumer is nil")
			}
			got, err := trie.Search("a")
			if err != nil || !maps.Equal(countConsumers(got), countConsumers([]*consumer.Consumer{subscriber})) {
				t.Errorf("Search(%q) after %s(%q, nil) = %v, error = %v, want [%p] and nil error", "a", operation.name, "a", got, err, subscriber)
			}
		})
	}
}

// TestConsumerTrieSubscribe checks pointer identity and duplicate rejection per prefix.
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
			first, second := &consumer.Consumer{}, &consumer.Consumer{}
			if err := trie.Subscribe(tt.prefix, first); err != nil {
				t.Errorf("Subscribe(%q, %p) error = %v, want nil", tt.prefix, first, err)
				return
			}
			if err := trie.Subscribe(tt.prefix, first); err == nil || err.Error() != "value already exists" {
				t.Errorf("Subscribe(%q, %p) a second time error = %v, want %q", tt.prefix, first, err, "value already exists")
			}
			if err := trie.Subscribe(tt.prefix, second); err != nil {
				t.Errorf("Subscribe(%q, %p) after subscribing %p error = %v, want nil", tt.prefix, second, first, err)
				return
			}
			if err := trie.Subscribe(tt.prefix+"/child", first); err != nil {
				t.Errorf("Subscribe(%q, %p) after subscribing at %q error = %v, want nil", tt.prefix+"/child", first, tt.prefix, err)
				return
			}
			key := tt.prefix + "/child/item"
			want := []*consumer.Consumer{first, second}
			got, err := trie.Search(key)
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(want)) {
				t.Errorf("Search(%q) = %v, error = %v, want distinct consumers %v and nil error", key, got, err, want)
			}
		})
	}
}

// TestConsumerTrieSearch checks matching ancestor prefixes and consumer deduplication.
func TestConsumerTrieSearch(t *testing.T) {
	first, second, third, other := &consumer.Consumer{}, &consumer.Consumer{}, &consumer.Consumer{}, &consumer.Consumer{}
	tests := []struct {
		name    string
		entries map[string][]*consumer.Consumer
		key     string
		want    []*consumer.Consumer
	}{
		{name: "empty trie", key: "abc"},
		{name: "exact match", entries: map[string][]*consumer.Consumer{"a": {first}}, key: "a", want: []*consumer.Consumer{first}},
		{name: "root matches unknown path", entries: map[string][]*consumer.Consumer{"": {first}}, key: "unknown", want: []*consumer.Consumer{first}},
		{name: "empty key", entries: map[string][]*consumer.Consumer{"": {first}, "a": {second}}, want: []*consumer.Consumer{first}},
		{
			name: "nested prefixes", entries: map[string][]*consumer.Consumer{"a": {first}, "ab": {second}, "abc": {third}, "x": {other}},
			key: "abcd", want: []*consumer.Consumer{first, second, third},
		},
		{name: "key shorter than subscription", entries: map[string][]*consumer.Consumer{"abc": {first}}, key: "ab"},
		{name: "descendant excluded", entries: map[string][]*consumer.Consumer{"a": {first}, "ab": {second}}, key: "a", want: []*consumer.Consumer{first}},
		{name: "missing suffix", entries: map[string][]*consumer.Consumer{"a": {first}, "abcd": {second}}, key: "abx", want: []*consumer.Consumer{first}},
		{name: "unrelated key", entries: map[string][]*consumer.Consumer{"a": {first}}, key: "b"},
		{
			name: "deduplicate across prefixes", entries: map[string][]*consumer.Consumer{"": {first}, "a": {first, second}, "ab": {first, third}},
			key: "abc", want: []*consumer.Consumer{first, second, third},
		},
		{
			name: "UTF-8", entries: map[string][]*consumer.Consumer{"é": {first}, "é/猫": {second}, "è": {other}},
			key: "é/猫/item", want: []*consumer.Consumer{first, second},
		},
		{name: "binary bytes", entries: map[string][]*consumer.Consumer{"\x00": {first}, "\x00\xff": {second}}, key: "\x00\xff/item", want: []*consumer.Consumer{first, second}},
		{name: "case sensitive", entries: map[string][]*consumer.Consumer{"Ab": {first}}, key: "ab"},
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
	first, second := &consumer.Consumer{}, &consumer.Consumer{}
	tests := []struct {
		name       string
		entries    map[string][]*consumer.Consumer
		prefix     string
		subscriber *consumer.Consumer
		key        string
		want       []*consumer.Consumer
		wantErr    string
	}{
		{name: "only subscription", entries: map[string][]*consumer.Consumer{"a": {first}}, prefix: "a", subscriber: first, key: "a"},
		{name: "other consumer remains", entries: map[string][]*consumer.Consumer{"a": {first, second}}, prefix: "a", subscriber: first, key: "a", want: []*consumer.Consumer{second}},
		{name: "descendant remains", entries: map[string][]*consumer.Consumer{"a": {first}, "ab": {second}}, prefix: "a", subscriber: first, key: "abc", want: []*consumer.Consumer{second}},
		{name: "ancestor remains", entries: map[string][]*consumer.Consumer{"a": {first}, "ab": {second}}, prefix: "ab", subscriber: second, key: "abc", want: []*consumer.Consumer{first}},
		{name: "same consumer at another prefix", entries: map[string][]*consumer.Consumer{"a": {first}, "ab": {first}}, prefix: "a", subscriber: first, key: "abc", want: []*consumer.Consumer{first}},
		{name: "root subscription", entries: map[string][]*consumer.Consumer{"": {first}, "a": {second}}, subscriber: first, key: "a", want: []*consumer.Consumer{second}},
		{name: "root only", entries: map[string][]*consumer.Consumer{"": {first}}, subscriber: first},
		{
			name: "missing prefix", entries: map[string][]*consumer.Consumer{"a": {first}}, prefix: "missing", subscriber: first,
			key: "a", want: []*consumer.Consumer{first}, wantErr: `prefix "missing" not found in trie`,
		},
		{
			name: "missing consumer", entries: map[string][]*consumer.Consumer{"a": {first}}, prefix: "a", subscriber: second,
			key: "a", want: []*consumer.Consumer{first}, wantErr: "value not found in trie",
		},
		{name: "empty trie", prefix: "a", subscriber: first, key: "a", wantErr: `prefix "a" not found in trie`},
		{name: "UTF-8", entries: map[string][]*consumer.Consumer{"é": {first}, "é/猫": {second}}, prefix: "é", subscriber: first, key: "é/猫", want: []*consumer.Consumer{second}},
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
					t.Errorf("Unsubscribe(%q, %p) with subscriptions %v error = %v, want %q", tt.prefix, tt.subscriber, tt.entries, err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("Unsubscribe(%q, %p) with subscriptions %v error = %v, want nil", tt.prefix, tt.subscriber, tt.entries, err)
			}
			got, err := trie.Search(tt.key)
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(tt.want)) {
				t.Errorf("Search(%q) after Unsubscribe(%q, %p) = %v, error = %v, want consumers %v and nil error", tt.key, tt.prefix, tt.subscriber, got, err, tt.want)
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
			want := make([]*consumer.Consumer, 0, tt.count)
			for i := 0; i < tt.count; i++ {
				subscriber := &consumer.Consumer{}
				want = append(want, subscriber)
				for _, prefix := range []string{"topic", "topic/events"} {
					if err := trie.Subscribe(prefix, subscriber); err != nil {
						t.Errorf("Subscribe(%q, %p) at index %d of %d error = %v, want nil", prefix, subscriber, i, tt.count, err)
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
					t.Errorf("Unsubscribe(%q, %p) with %d consumers error = %v, want nil", "topic", subscriber, tt.count, err)
					return
				}
			}
			got, err = trie.Search("topic/events/item")
			if err != nil || !maps.Equal(countConsumers(got), countConsumers(want)) {
				t.Errorf("Search(%q) after removing %d parent subscriptions = %v, error = %v, want consumers %v and nil error", "topic/events/item", tt.count, got, err, want)
			}
			for _, subscriber := range want {
				if err := trie.Unsubscribe("topic/events", subscriber); err != nil {
					t.Errorf("Unsubscribe(%q, %p) with %d consumers error = %v, want nil", "topic/events", subscriber, tt.count, err)
					return
				}
			}
			if got, err := trie.Search("topic/events/item"); err != nil || len(got) != 0 {
				t.Errorf("Search(%q) after removing all %d consumers = %v, error = %v, want no consumers and nil error", "topic/events/item", tt.count, got, err)
			}
		})
	}
}
