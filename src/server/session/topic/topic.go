package topic

import (
	"tree_nity/src/communication/protocol/message"
	"tree_nity/src/server/session/consumer"
)

type Topic struct {
	messages          []protocol.Message
	messageOffsetTrie MessageOffsetTrie
	consumerTrie      ConsumerTrie
	newConsumerChan   chan consumer.Consumer
	shuttingDownChan  chan struct{}
	newMessageChan    chan struct{}
}
