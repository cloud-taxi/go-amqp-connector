package amqp_lib

import (
	"fmt"
	"math/rand"
	"time"
)

func RandomQueueName(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, RandomString(16))
}

func RandomString(strlen int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}
