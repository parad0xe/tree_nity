package topic

import (
	"errors"

	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/trie"
)

type ConsumerTrie struct {
	trie *trie.Trie[*consumer.Consumer]
}

func NewConsumerTrie() *ConsumerTrie {
	return &ConsumerTrie{
		trie: trie.NewTrie[*consumer.Consumer](),
	}
}

func (t *ConsumerTrie) Subscribe(prefix string, consumer *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if consumer == nil {
		return errors.New("consumer is nil")
	}

	return t.trie.Insert(prefix, consumer)
}

func (t *ConsumerTrie) Unsubscribe(prefix string, consumer *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if consumer == nil {
		return errors.New("consumer is nil")
	}

	return t.trie.Remove(prefix, consumer)
}

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
