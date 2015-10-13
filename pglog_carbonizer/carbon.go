package pglog_carbonizer

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/hpcloud/tail"
	"github.com/marpaia/graphite-golang"
	processor "github.com/loggi/pglog-processor/types"
	"strings"
)

type Config struct {
	Main struct {
		Enabled      bool   // enables the actual sending or uses a nop sender
		InputLogFile string // file to be watched and read
		LogLevel     string // log level
		Lines        int    // number of lines to be read from log. 0 to unlimited.
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

type Watcher struct {
	Tail *tail.Tail
}

// Creates a Watcher around a watched file.
func WatchLog(watched string) Watcher {
	tail, err := tail.TailFile(watched, tail.Config{Follow: true, ReOpen: true })
	if err != nil {
		log.WithError(err).Fatal()
		panic(err)
	}
	return Watcher{ Tail : tail }
}

// Watch a log file, munching new lines read.
func (w Watcher) Watch(munch Muncher, conf Config) {
	i := 0
	for line := range w.Tail.Lines {
		if line.Text != "" {
			if err := munch(line.Text, conf.Graphite.Prefix); err != nil {
				log.WithError(err).Error()
			}
		}
		if i++; i == conf.Main.Lines {
			break
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
