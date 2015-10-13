package pglog_carbonizer

import (
	"github.com/marpaia/graphite-golang"
	"testing"
	"fmt"
	"os"
	"io/ioutil"
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"time"
	"github.com/hpcloud/tail"
)

const (
	logfile = "/tmp/pglog-carbonizer.log"
)

var JSON_DATA = `{"action":"PgNormalizedQueries","@timestamp":"2015-10-09T18:00:00+00:00","duration":115,"query":"select 1","count":1}`

func TestMuncher(t *testing.T) {
	if err := NewGraphiteSender(graphite.NewGraphiteNop("localhost", 123))(JSON_DATA, "xpto"); err != nil {
		t.Error(err)
	}
}

func TestWatchLog(t *testing.T) {
	conf := Config{}
	conf.Main.Lines = 1
	WatchLog("/tmp/pglog-carbonizer.log").Watch(func(line string, prefix string) error {
		fmt.Println()
		return nil
	}, conf)
}

// Makes tests over tail implementation choosed easier.
func TestTail(t *testing.T) {
	// end of tail
	eot := "EOT"

	if err := createFile(logfile); err != nil {
		log.Fatal(err)
		t.Fail()
	}

	tail, err := tail.TailFile(logfile, tail.Config{
		Follow: true,
		ReOpen: true,
	})

	if err != nil {
		t.Error(err)
	}

	go func() {
		log.Debug("starting")
		for line := range tail.Lines {
			log.Debug(line)
			var en interface{}
			if err := json.Unmarshal([]byte(line.Text), &en); err != nil {
				log.Debug(err)
			}
			if line.Text == eot {
				log.Debug("ending")
				return
			}
		}
	}()

	// writing to watched logs
	for i := 0; i < 10; i++ {
		if err := writeLine(logfile, fmt.Sprintf("line %#v\n", i)); err != nil {
			t.Error(err)
			break
		}
	}

	// ZZzzzz..
	time.Sleep(2 * time.Second)

	// ends the watching
	writeLine(logfile, eot)
}

func createFile(fname string) error {
	return ioutil.WriteFile(fname, []byte{}, 0600)
}

func writeLine(fname string, contents string) (err error) {
	f, err := os.OpenFile(fname, os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		 return err
	}
	defer f.Close()
	if _, err := f.WriteString(contents); err != nil {
		return err
	}
	f.Sync()
	return nil
}