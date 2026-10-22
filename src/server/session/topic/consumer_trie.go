package topic

import (
	"errors"
	"sync"

	"tree_nity/src/server/utils"
	"tree_nity/src/server/utils/trie"
)

// ConsumerTrie indexes consumer subscriptions by byte prefix.
type ConsumerTrie struct {
	mu   sync.RWMutex
	trie *trie.Trie[id.ID]
}

// NewConsumerTrie returns an empty, initialized subscription index.
func NewConsumerTrie() *ConsumerTrie {
	return &ConsumerTrie{
		trie: trie.NewTrie[id.ID](),
	}
}

// Subscribe associates consumer with prefix, rejecting empty IDs and duplicates.
func (t *ConsumerTrie) Subscribe(prefix string, consumer id.ID) error {
	if err := t.validate(); err != nil {
		return err
	}
	if consumer == "" {
		return errors.New("consumer ID is empty")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.trie.Insert(prefix, consumer)
}

// Unsubscribe removes consumer's subscription at prefix, returning an error if absent.
func (t *ConsumerTrie) Unsubscribe(prefix string, consumer id.ID) error {
	if err := t.validate(); err != nil {
		return err
	}
	if consumer == "" {
		return errors.New("consumer ID is empty")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.trie.Remove(prefix, consumer)
}

// Search returns each consumer subscribed to a prefix of key at most once.
func (t *ConsumerTrie) Search(key string) ([]id.ID, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}

	t.mu.RLock()
	defer t.mu.RUnlock()
	consumers := make([]id.ID, 0)
	seen := make(map[id.ID]struct{})

	_, err := t.trie.MapPath(key, func(node *trie.Node[id.ID]) {
		for _, c := range node.Values() {
			if _, exists := seen[c]; exists {
				continue
			}
			seen[c] = struct{}{}
			consumers = append(consumers, c)
		}
	}, nil)
	if err != nil {
		return nil, err
	}

	return consumers, nil
}

func (t *ConsumerTrie) validate() error {
	if t == nil || t.trie == nil {
		return errors.New("consumer trie is not initialized")
	}
	return nil
}
