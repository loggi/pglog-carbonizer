package pglog_carbonizer

import (
	"github.com/marpaia/graphite-golang"
	"testing"
)

var JSON_DATA = `{"action":"PgNormalizedQueries","@timestamp":"2015-10-09T18:00:00+00:00","duration":115,"query":"select 1","count":1}`

func TestMuncher(t *testing.T) {
	if err := NewGraphiteSender(graphite.NewGraphiteNop("localhost", 123))(JSON_DATA, "xpto"); err != nil {
		t.Error(err)
	}
}

// TODO fix testing functions which run goroutines..
//func TestWatchLog(t *testing.T) {
//	conf := Config{
//		TailTimeout: 1,
//	}
//	WatchLog("/tmp/batata/teste.log", func(line string, prefix string) error {
//		fmt.Println()
//		return nil
//	}, conf)
//}
