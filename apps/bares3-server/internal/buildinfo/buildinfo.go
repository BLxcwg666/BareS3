package buildinfo

import (
	"fmt"
	"runtime"
	"strings"

	"bares3-server/internal/config"
)

var (
	Version = "1.2.0"
	Commit  = "unknown"
	BuiltAt = "unknown"
)

type Info struct {
	Product   string `json:"product"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuiltAt   string `json:"built_at"`
	GoVersion string `json:"go_version"`
}

func Current() Info {
	return Info{
		Product:   config.ProductName,
		Version:   Version,
		Commit:    Commit,
		BuiltAt:   BuiltAt,
		GoVersion: runtime.Version(),
	}
}

func (i Info) String() string {
	parts := []string{fmt.Sprintf("%s %s", i.Product, i.Version)}
	if strings.TrimSpace(i.Commit) != "" && i.Commit != "unknown" {
		parts = append(parts, "commit "+i.Commit)
	}
	if strings.TrimSpace(i.BuiltAt) != "" && i.BuiltAt != "unknown" {
		parts = append(parts, "built "+i.BuiltAt)
	}
	parts = append(parts, i.GoVersion)
	return strings.Join(parts, " | ")
}
