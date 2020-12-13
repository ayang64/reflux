package fluxql_test

import (
	"context"
	"strings"
	"testing"

	"github.com/ayang64/reflux/fluxql"
)

func TestScan(t *testing.T) {
	scn := fluxql.Scanner{}
	for tok := range scn.Scan(context.Background(), strings.NewReader(`select *, "hello, world", 'hello,\\ \' world 2', b5, x6, 911, 1.5 from foo;`)) {
		t.Logf("%s", tok)
	}
}

func TestParse(t *testing.T) {
	p := fluxql.Parser{}
	p.Parse(context.Background(), strings.NewReader(`select foo, foo.*, *, "hello, world", 'hello,\\ \' world 2', b5, x6, 911, 1.5 from foo;`))
	p.Parse(context.Background(), strings.NewReader(`insert (a, b, c) into foobar values (a, b, c, d)`))
}
