package pglog_carbonizer

import (
	"fmt"
	"testing"
)

var JSON_DATA = `{"action":"PgNormalizedQueries","@timestamp":"2015-10-09T18:00:00+00:00","duration":115,"query":"select 1","count":1}`

func TestWatchLog(t *testing.T) {
	watchLog("/tmp/batata/teste.log", func(line string) error {
		fmt.Println()
		return nil
	})
}

func TestMuncher(t *testing.T) {
	if err := muncher(JSON_DATA); err != nil {
		t.Error(err)
	}
}
