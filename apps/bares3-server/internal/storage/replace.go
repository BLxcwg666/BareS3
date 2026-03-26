package storage

import (
	"errors"
	"os"
)

func replaceFile(fromPath, toPath string) error {
	if err := os.Rename(fromPath, toPath); err == nil {
		return nil
	}

	if err := os.Remove(toPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return os.Rename(fromPath, toPath)
}
