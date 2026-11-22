package streaming

import (
	"reflect"
	"testing"

	"github.com/neofactory/streaming-parquet/parquet_checkpoint/go/s3fake"
)

func TestParseS3Root(t *testing.T) {
	for _, c := range []struct {
		root, bucket, prefix string
		ok                   bool
	}{
		{"s3://b", "b", "", true},
		{"s3://b/", "b", "", true},
		{"s3://b/tele", "b", "tele/", true},
		{"s3://b/tele/x/", "b", "tele/x/", true},
		{"s3://", "", "", false},
		{"file:///data", "", "", false},
		{"/data", "", "", false},
	} {
		b, p, ok := parseS3Root(c.root)
		if b != c.bucket || p != c.prefix || ok != c.ok {
			t.Errorf("%q: %q %q %v, want %q %q %v", c.root, b, p, ok, c.bucket, c.prefix, c.ok)
		}
	}
}

func TestS3StoreTickets(t *testing.T) {
	fake := s3fake.New()
	defer fake.Close()
	store := newS3Store(fake.Client(), "b", "tele/")
	if got, err := store.get("air/dfw"); err != nil || got != nil {
		t.Fatalf("absent ticket: %v %v", got, err)
	}
	want := Ticket{Name: "air/dfw", Host: "10.0.0.5", Port: 8815, Ts: 1760745600, Run: "2026-10-26T20:31:12.123456Z", Archive: "s3://b/tele/archive"}
	if err := store.put("air/dfw", want); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.Object("b", "tele/tickets/air/dfw.json"); !ok {
		t.Fatal("the ticket is not at <prefix>/tickets/<name>.json")
	}
	got, err := store.get("air/dfw")
	if err != nil || got == nil || !reflect.DeepEqual(*got, want) {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	if err := store.put("air/hou", Ticket{Name: "air/hou"}); err != nil {
		t.Fatal(err)
	}
	names, err := store.list()
	if err != nil || len(names) != 2 || names[0] != "air/dfw" || names[1] != "air/hou" {
		t.Fatalf("list %v %v", names, err)
	}
	if err := store.delete("air/dfw"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.get("air/dfw"); got != nil {
		t.Fatal("deleted ticket still read")
	}
	if err := store.delete("air/dfw"); err != nil {
		t.Fatalf("deleting an absent ticket is not an error: %v", err)
	}
}
