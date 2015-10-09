package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"github.com/marpaia/graphite-golang"
	carbon "loggi/pglog-carbonizer/pglog_carbonizer"
	"gopkg.in/gcfg.v1"
)

const (
	defaultConfigFile = "pglog-carbonizer.conf"
)

var gcon *graphite.Graphite

var conf carbon.Config

func init() {

	var confPath string
	flag.StringVar(&confPath, "conf", defaultConfigFile, "Config file path")
	flag.Parse()

	// loading config
	log.WithField("Using configuration file", confPath).Info()
	err := gcfg.ReadFileInto(&conf, confPath)
	carbon.CheckAndPanic(err, "Couldn't read configuration file", log.Fields{})
	level, err := log.ParseLevel(conf.Main.LogLevel)
	carbon.CheckAndPanic(err, "Couldn't set debug level", log.Fields{"level": conf.Main.LogLevel})

	// using some config values
	log.SetLevel(level)
	if conf.Main.Enabled {
		gcon, err = graphite.NewGraphite(conf.Graphite.Host, conf.Graphite.Port)
		carbon.CheckAndPanic(err, "Couldn't read configuration file", log.Fields{
			"host": conf.Graphite.Host,
			"port": conf.Graphite.Port,
		})
	} else {
		gcon = graphite.NewGraphiteNop(conf.Graphite.Host, conf.Graphite.Port)
	}

	log.WithField("Connection", gcon).Info()
}

func main() {
	carbon.WatchLog(conf.Main.InputLogFile, carbon.NewGraphiteSender(gcon), conf)
}
