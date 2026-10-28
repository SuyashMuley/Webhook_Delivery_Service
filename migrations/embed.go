// Package migrations embeds the SQL migrations so the migrate binary is self-contained.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
