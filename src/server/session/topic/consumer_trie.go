package topic

import (
	"errors"

	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/trie"
)

// ConsumerTrie indexes consumer subscriptions by byte prefix.
type ConsumerTrie struct {
	trie *trie.Trie[*consumer.Consumer]
}

// NewConsumerTrie returns an empty, initialized subscription index.
func NewConsumerTrie() *ConsumerTrie {
	return &ConsumerTrie{
		trie: trie.NewTrie[*consumer.Consumer](),
	}
}

// Subscribe associates consumer with prefix, rejecting nil consumers and duplicates.
func (t *ConsumerTrie) Subscribe(prefix string, consumer *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if consumer == nil {
		return errors.New("consumer is nil")
	}

	return t.trie.Insert(prefix, consumer)
}

// Unsubscribe removes consumer's subscription at prefix, returning an error if absent.
func (t *ConsumerTrie) Unsubscribe(prefix string, consumer *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if consumer == nil {
		return errors.New("consumer is nil")
	}

	return t.trie.Remove(prefix, consumer)
}

// Search returns each consumer subscribed to a prefix of key at most once.
func (t *ConsumerTrie) Search(key string) ([]*consumer.Consumer, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}

	consumers := make([]*consumer.Consumer, 0)
	seen := make(map[*consumer.Consumer]struct{})

	_, err := t.trie.MapPath(key, func(node *trie.Node[*consumer.Consumer]) {
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
