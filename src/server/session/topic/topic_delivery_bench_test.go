package topic

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/session/consumer"
)

// BenchmarkDeliveryLiveOffsets measures storage and live handoff after catching up.
func BenchmarkDeliveryLiveOffsets(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("stored=%d", count), func(b *testing.B) {
			target := newTopic()
			for index := 0; index < count; index++ {
				message := protocol.NewMessage([]byte(fmt.Sprintf("user.%d", index)), nil)
				if err := target.storeMessage(message); err != nil {
					b.Fatalf("storeMessage(history %d) error = %v, want nil", index, err)
				}
			}
			path := fmt.Sprintf("/delivery-bench.%d.%s", os.Getpid(), strings.ReplaceAll(b.Name(), "/", "."))
			inbound, err := ipc.NewInbound(path, protocol.Decode)
			if err != nil {
				b.Fatalf("NewInbound(%q) during setup error = %v, want nil", path, err)
			}
			b.Cleanup(func() {
				if err := inbound.Drop(); err != nil {
					b.Errorf("Drop(%q) error = %v, want nil", path, err)
				}
			})
			manager := consumer.NewManager()
			startOffset := uint32(count)
			c, _, err := manager.Subscribe(context.Background(), "subscriber", "events", &startOffset, "", path)
			if err != nil {
				b.Fatalf("Subscribe(subscriber, offset %d) during setup error = %v, want nil", startOffset, err)
			}
			b.Cleanup(func() {
				if err := manager.DisconnectConsumer(c); err != nil {
					b.Errorf("DisconnectConsumer(subscriber) error = %v, want nil", err)
				}
			})
			// Register without workers to measure handoff independently of IPC and ACK latency.
			if err := target.consumerTrie.Subscribe("", c.Info.ID()); err != nil {
				b.Fatalf("consumerTrie.Subscribe(subscriber) error = %v, want nil", err)
			}
			if err := target.consumerMap.Insert(c.Info.ID(), &subscription{info: c.Info, connection: c.Connection()}); err != nil {
				b.Fatalf("consumerMap.Insert(subscriber) error = %v, want nil", err)
			}
			connection := c.Connection()
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				offset := count + index
				message := protocol.NewMessage([]byte(fmt.Sprintf("user.%d", offset)), nil)
				if err := target.storeMessage(message); err != nil {
					b.Fatalf("storeMessage(live %d) error = %v, want nil", offset, err)
				}
				batch := <-connection.MessageChan
				if len(batch) != 1 || batch[0] != uint32(offset) {
					b.Errorf("live batch = %v, want [%d]", batch, offset)
				}
			}
		})
	}
}
