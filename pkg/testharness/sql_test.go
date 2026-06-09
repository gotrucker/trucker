package testharness

import (
	"reflect"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "basic",
			sql:  "SELECT 1; SELECT 2;",
			want: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name: "quotes",
			sql:  `INSERT INTO x VALUES ('a;b', "weird;name"); SELECT 1;`,
			want: []string{`INSERT INTO x VALUES ('a;b', "weird;name")`, "SELECT 1"},
		},
		{
			name: "dollar quotes",
			sql:  "SELECT $$a;b$$; SELECT $tag$c;d$tag$;",
			want: []string{"SELECT $$a;b$$", "SELECT $tag$c;d$tag$"},
		},
		{
			name: "line comments",
			sql:  "SELECT 1 -- ; ignored\n; SELECT 2;",
			want: []string{"SELECT 1 -- ; ignored", "SELECT 2"},
		},
		{
			name: "empty statements",
			sql:  " ;\n; SELECT 1 ; ;",
			want: []string{"SELECT 1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.sql)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}
