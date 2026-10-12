package session

import (
	"tree_nity/src/communication/protocol/message"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/prefix_tree"
)

type Topic struct {
	messages           []protocol.Message
	consumerPrefixTree utils.PrefixTree
	messagePrefixTree  utils.PrefixTree
	newConsumerChan    chan session.Consumer
	shuttingDownChan   chan struct{}
	newMessageChan     chan struct{}
}
