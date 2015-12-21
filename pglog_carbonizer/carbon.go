package pglog_carbonizer

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/hpcloud/tail"
	processor "github.com/loggi/pglog-processor/types"
	"github.com/marpaia/graphite-golang"
	"strings"
	"time"
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
// Creates a Muncher specific to a entry type: NormalizedInfoEntry
func NewGraphiteSender(gcon *graphite.Graphite) Muncher {
	return func(jsonData string, prefix string) error {
		gcon.Connect()

		if icontains(jsonData, processor.PmiActionKeyOnES) {
			log.WithField("key", processor.PmiActionKeyOnES).Debug("Sent")
			return sendPerMinuteInfoEntry(jsonData, prefix, gcon)
		}

		log.Debug("Sent nothing")
		return nil
	}
}

func sendPerMinuteInfoEntry(jsonData string, prefix string, gcon *graphite.Graphite) error {
	var en = processor.PerMinuteInfoEntry{}
	if err := json.Unmarshal([]byte(jsonData), &en); err != nil {
		return err
	}
	log.WithField("entry", en).Debug()

	key := fmt.Sprintf("%s.%s.%s", prefix, strings.ToLower(en.Action), strings.ToLower(en.Desc))
	ts := time.Time(en.Timestamp).Unix()

	count := graphite.NewMetric(
		fmt.Sprintf("%s.count", key),
		fmt.Sprintf("%v", en.Count),
		ts)
	if err := gcon.SendMetric(count); err != nil {
		log.WithField("metric", count).Error(err)
		return err
	} else {
		log.WithField("sent", fmt.Sprintf("%s %s %d\n", count.Name, count.Value, count.Timestamp)).Info("sent")
	}

	duration := graphite.NewMetric(
		fmt.Sprintf("%s.duration", key),
		fmt.Sprintf("%v", en.Duration),
		ts)

	if err := gcon.SendMetric(duration); err != nil {
		log.WithField("metric", duration).Error(err)
		return err
	} else {
		log.WithField("sent", fmt.Sprintf("%s %s %d\n", duration.Name, duration.Value, duration.Timestamp)).Info("sent")
	}

	log.WithFields(log.Fields{
		"count":    count,
		"duration": duration,
	}).Info("Sending metrics")

	return gcon.SendMetric(count)
}

func sendNormalizedInfoEntry(jsonData string, prefix string, gcon *graphite.Graphite) error {
	var en = processor.NormalizedInfoEntry{}
	if err := json.Unmarshal([]byte(jsonData), &en); err != nil {
		return err
	}
	log.WithField("entry", en).Debug()

	key := fmt.Sprintf("%s.%s", prefix, strings.ToLower(en.Action))
	ts := time.Time(en.Timestamp).Unix()

	count := graphite.NewMetric(
		fmt.Sprintf("%s.count", key),
		fmt.Sprintf("%v", en.Count),
		ts)
	if err := gcon.SendMetric(count); err != nil {
		log.WithField("metric", count).Error(err)
		return err
	} else {
		log.WithField("sent", fmt.Sprintf("%s %s %d\n", count.Name, count.Value, count.Timestamp)).Info("sent")
	}

	duration := graphite.NewMetric(
		fmt.Sprintf("%s.duration", key),
		fmt.Sprintf("%v", en.Duration),
		ts)

	if err := gcon.SendMetric(duration); err != nil {
		log.WithField("metric", duration).Error(err)
		return err
	} else {
		log.WithField("sent", fmt.Sprintf("%s %s %d\n", duration.Name, duration.Value, duration.Timestamp)).Info("sent")
	}

	log.WithFields(log.Fields{
		"count":    count,
		"duration": duration,
	}).Info("Sending metrics")

	return gcon.SendMetric(count)
}

type Watcher struct {
	Tail *tail.Tail
}

// Creates a Watcher around a watched file.
func WatchLog(watched string) Watcher {
	tail, err := tail.TailFile(watched, tail.Config{Follow: true, ReOpen: true})
	if err != nil {
		log.WithError(err).Fatal()
		panic(err)
	}
	return Watcher{Tail: tail}
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

func icontains(s string, substr string) bool {
	// in this context 's' is usually short,
	// otherwise a regexp would be more appropriate
	lo_s := strings.ToLower(s)
	lo_substr := strings.ToLower(substr)
	return strings.Contains(lo_s, lo_substr)
}
