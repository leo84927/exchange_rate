package handle

import (
	"log"

	"github.com/leo84927/rabbitmq"
)

func MessageHandler(msg rabbitmq.Message) error {
	log.Printf("=== Start processing message ===")
	log.Printf("Message body: %s", msg.Body)
	log.Printf("=== End processing message ===")
	return nil
}
