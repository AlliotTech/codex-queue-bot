package web

import (
	"embed"
	"io/fs"
)

// The frontend build writes into ui/dist before the Go binary is compiled.
// A tiny checked-in fallback keeps ordinary `go test ./...` and local backend
// development working even when Node has not been run yet.
//
//go:embed ui/dist
var embeddedUI embed.FS

func uiFileSystem() fs.FS {
	files, err := fs.Sub(embeddedUI, "ui/dist")
	if err != nil {
		panic(err)
	}
	return files
}
