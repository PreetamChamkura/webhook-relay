// Package migrations embeds the golang-migrate SQL files.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
