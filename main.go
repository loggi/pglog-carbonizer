package pglog_carbonizer

import (
	"code.google.com/p/gcfg"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/dleung/gotail"
	"github.com/marpaia/graphite-golang"
	pglogproc "loggi/pglog-processor/types"
	"strings"
)

const (
	defaultConfigFile = "pglog-carbonizer.conf"
)

type Config struct {
	Main struct {
		Enabled      bool   // enables the actual sending or uses a nop sender
		InputLogFile string // file to be watched and read
		LogLevel     string // log level
		Timeout      int    // timeout in seconds
	}
	graphite.Graphite
}

var gcon *graphite.Graphite

var conf Config

func init() {

	var confPath string
	flag.StringVar(&confPath, "conf", defaultConfigFile, "Config file path")
	flag.Parse()

	// loading config
	log.WithField("Using configuration file", confPath).Info()
	err := gcfg.ReadFileInto(&conf, confPath)
	checkAndPanic(err, "Couldn't read configuration file", log.Fields{})
	level, err := log.ParseLevel(conf.Main.LogLevel)
	checkAndPanic(err, "Couldn't set debug level", log.Fields{"level": conf.Main.LogLevel})

	// using some config values
	log.SetLevel(level)
	if conf.Main.Enabled {
		gcon, err = graphite.NewGraphite(conf.Graphite.Host, conf.Graphite.Port)
		checkAndPanic(err, "Couldn't read configuration file", log.Fields{
			"host": conf.Graphite.Host,
			"port": conf.Graphite.Port,
		})
	} else {
		gcon = graphite.NewGraphiteNop(conf.Graphite.Host, conf.Graphite.Port)
	}

	log.WithField("Connection", gcon).Info()
}

func main() {
	watchLog(conf.Main.InputLogFile, graphiteSender)
}

// Munch... munch...
type muncher func(jsonData string) error

// Sends data extracted from json to graphite's carbon.
func graphiteSender(jsonData string) error {
	var en = pglogproc.NormalizedInfoEntry{}
	if err := json.Unmarshal([]byte(jsonData), &en); err != nil {
		return err
	}
	key := fmt.Sprintf("%s.%s", conf.Graphite.Prefix, strings.ToLower(en.Action))
	log.WithField("entry", en).Info()
	if err := gcon.SimpleSend(fmt.Sprintf("%s.count", key), fmt.Sprintf("%v", en.Count)); err != nil {
		return err
	}
	if err := gcon.SimpleSend(fmt.Sprintf("%s.duration", key), en.Duration.String()); err != nil {
		return err
	}
	return nil
}

// Watches the given log, munching new lines read.
func watchLog(watched string, munch muncher) {

	// Set timeout to 0 to automatically fail if file isn't found
	tail, err := gotail.NewTail(watched, gotail.Config{Timeout: conf.Main.Timeout})
	if err != nil {
		log.WithError(err).Fatal()
	}

	for line := range tail.Lines {
		if line != "" {
			munch(line)
		}
	}
}

// Simple error checking. Wraps log utilities.
func checkAndPanic(err error, panicMsg string, panicFields log.Fields) {
	if err == nil {
		return
	}
	log.WithError(err).Error()
	log.WithFields(panicFields).Panic(panicMsg)
	panic(panicMsg)
}
