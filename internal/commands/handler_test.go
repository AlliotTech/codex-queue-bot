package commands

import (
	"reflect"
	"testing"
)

func TestParseCommandAndTargets(t *testing.T) {
	command, args, ok := parseCommand("  /开挤@bot  a,b，c  ")
	if !ok || command != "开挤" || args != "a,b，c" {
		t.Fatalf("parseCommand = %q %q %v", command, args, ok)
	}
	want := []string{"a", "b", "c"}
	if got := splitTargets(args); !reflect.DeepEqual(got, want) {
		t.Fatalf("splitTargets = %#v, want %#v", got, want)
	}
}
