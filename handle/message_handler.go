package handle

import (
	"log"

	"github.com/leo84927/rabbitmq/v2"
)

func MessageHandler(msg rabbitmq.Message) (requeue bool, err error) {
	log.Printf("=== Start processing message ===")
	log.Printf("Message body: %s", msg.Body)
	log.Printf("=== End processing message ===")
	return true, nil
}
