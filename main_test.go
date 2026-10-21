package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestCollectCallEdgesKeepsMCPStdoutClean(t *testing.T) {
	original := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = original })

	var messages []string
	collectCallEdges(nil, func(string, string, string) {}, func(_ string, _, _ int, msg string) {
		messages = append(messages, msg)
	})

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("MCP stdout contained non-protocol output: %q", got)
	}
	if len(messages) == 0 || !strings.Contains(messages[0], "VTA") {
		t.Fatalf("expected VTA diagnostic on progress channel, got %q", messages)
	}
}
