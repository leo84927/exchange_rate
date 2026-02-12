package config

import "os"

var (
	ServiceName = os.Getenv("SERVICE_NAME")
)
