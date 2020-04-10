package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jetlwx/sniffer-agent/capture"
	"github.com/jetlwx/sniffer-agent/communicator"
	"github.com/jetlwx/sniffer-agent/exporter"
	sd "github.com/jetlwx/sniffer-agent/session-dealer"
	log "github.com/sirupsen/logrus"
)

var (
	logLevel string
)

func init() {
	flag.StringVar(&logLevel, "log_level", "warn", "log level. Default is info")
}

func initLog() {
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	switch logLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		panic(fmt.Sprintf("cannot set log level:%s, there have four types can set: debug, info, warn, error", logLevel))
	}
}

func main() {
	flag.Parse()
	initLog()
	sd.CheckParams()

	go communicator.Server()
	mainServer()
}

func mainServer() {
	ept := exporter.NewExporter()
	networkCard := capture.NewNetworkCard()
	log.Info("begin listen")
	for queryPiece := range networkCard.Listen() {
		err := ept.Export(queryPiece)
		if err != nil {
			log.Error(err.Error())
		}
		queryPiece.Recovery()
	}

	log.Errorf("cannot get network package from %s", capture.DeviceName)
	os.Exit(1)
}
