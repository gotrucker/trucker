package testharness

import "testing"

func TestIsTruthy(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{name: "bool true", value: true, want: true},
		{name: "int one", value: int64(1), want: true},
		{name: "uint8 one", value: uint8(1), want: true},
		{name: "bool false", value: false, want: false},
		{name: "zero", value: 0, want: false},
		{name: "nil", value: nil, want: false},
		{name: "string true", value: "true", want: false},
		{name: "string one", value: "1", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTruthy(tt.value); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckExpectationRow(t *testing.T) {
	failures := checkExpectationRow(2, "SELECT true, false", 3, []string{"ok", "bad"}, []any{true, false})
	if len(failures) != 1 {
		t.Fatalf("expected one failure, got %d", len(failures))
	}
	if failures[0].Reason != "non_truthy_cell" || failures[0].ColumnName != "bad" || failures[0].RowIndex != 3 {
		t.Fatalf("unexpected failure: %#v", failures[0])
	}
}
