package pulsarutil

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func GetStartOption(startFrom string) (pulsar.SubscriptionInitialPosition, *pulsar.MessageID, *time.Time, error) {
	switch {
	case startFrom == "earliest":
		return pulsar.SubscriptionPositionEarliest, nil, nil, nil
	case strings.HasPrefix(startFrom, "messageId:"):
		parts := strings.Split(strings.TrimPrefix(startFrom, "messageId:"), ":")
		if len(parts) != 2 {
			return 0, nil, nil, fmt.Errorf("invalid messageId format, expected ledgerId:entryId")
		}
		ledgerId, err1 := strconv.ParseInt(parts[0], 10, 64)
		entryId, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			return 0, nil, nil, fmt.Errorf("invalid messageId numbers")
		}
		msgID := pulsar.NewMessageID(ledgerId, entryId, -1)
		return 0, &msgID, nil, nil
	case strings.HasPrefix(startFrom, "timestamp:"):
		millisStr := strings.TrimPrefix(startFrom, "timestamp:")
		millis, err := strconv.ParseInt(millisStr, 10, 64)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("invalid timestamp")
		}
		t := time.UnixMilli(millis)
		return 0, nil, &t, nil
	default:
		return 0, nil, nil, fmt.Errorf("unsupported startFrom value")
	}
}
