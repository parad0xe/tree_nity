package response

import "errors"

// ErrReply reports a failure sending a reply or closing its outbound handle.
var ErrReply = errors.New("reply communication failed")
