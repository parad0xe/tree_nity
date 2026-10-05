package session

import (
	protocol "tree_nity/communication/protocol/message"
	session "tree_nity/server/session/consumer"
	utils "tree_nity/server/utils/prefix_tree"
)

type Topic struct {
	messages           []protocol.Message
	consumerPrefixTree utils.PrefixTree
	messagePrefixTree  utils.PrefixTree
	newConsumerChan    chan session.Consumer
	shuttingDownChan   chan struct{}
	newMessageChan     chan struct{}
}
