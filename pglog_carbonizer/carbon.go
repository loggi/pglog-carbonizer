package pglog_carbonizer

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/dleung/gotail"
	"github.com/marpaia/graphite-golang"
	processor "loggi/pglog-processor/types"
	"strings"
)

type Config struct {
	Main struct {
		Enabled      bool   // enables the actual sending or uses a nop sender
		InputLogFile string // file to be watched and read
		LogLevel     string // log level
		TailTimeout  int    // timeout in seconds
	}
	graphite.Graphite
}

// Munch... munch...
type Muncher func(jsonData string, prefix string) error

// Sends data extracted from json to graphite's carbon.
func NewGraphiteSender(gcon *graphite.Graphite) Muncher {
	return func(jsonData string, prefix string) error {
		var en = processor.NormalizedInfoEntry{}
		if err := json.Unmarshal([]byte(jsonData), &en); err != nil {
			return err
		}
		key := fmt.Sprintf("%s.%s", prefix, strings.ToLower(en.Action))
		log.WithField("entry", en).Info()
		if err := gcon.SimpleSend(fmt.Sprintf("%s.count", key), fmt.Sprintf("%v", en.Count)); err != nil {
			return err
		}
		if err := gcon.SimpleSend(fmt.Sprintf("%s.duration", key), en.Duration.String()); err != nil {
			return err
		}
		return nil
	}
}

// Watches the given log, munching new lines read.
func WatchLog(watched string, munch Muncher, conf Config) {

	// Set timeout to 0 to automatically fail if file isn't found
	tail, err := gotail.NewTail(watched, gotail.Config{Timeout: conf.Main.TailTimeout})
	if err != nil {
		log.WithError(err).Fatal()
	}

	for line := range tail.Lines {
		if line != "" {
			if err := munch(line, conf.Graphite.Prefix); err != nil {
				log.WithError(err).Error()
			}
		}
	}
}

// Simple error checking. Wraps log utilities.
func CheckAndPanic(err error, panicMsg string, panicFields log.Fields) {
	if err == nil {
		return
	}
	log.WithError(err).Error()
	log.WithFields(panicFields).Panic(panicMsg)
	panic(panicMsg)
}
