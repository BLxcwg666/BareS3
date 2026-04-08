package main

import (
	"bufio"
	"strings"
	"testing"
)

func TestPromptPasswordFallsBackToBufferedInput(t *testing.T) {
	originalReader := stdinReader
	originalFD := stdinFD
	originalIsTerminal := stdinIsTerminal
	originalReadPassword := readPassword
	defer func() {
		stdinReader = originalReader
		stdinFD = originalFD
		stdinIsTerminal = originalIsTerminal
		readPassword = originalReadPassword
	}()

	stdinReader = bufio.NewReader(strings.NewReader("secret-password\n"))
	stdinFD = func() int { return 0 }
	stdinIsTerminal = func(fd int) bool { return false }
	readPassword = func(fd int) ([]byte, error) {
		t.Fatalf("terminal password reader should not be used for non-terminal input")
		return nil, nil
	}

	password, err := promptPassword("Password: ")
	if err != nil {
		t.Fatalf("promptPassword failed: %v", err)
	}
	if password != "secret-password" {
		t.Fatalf("unexpected password: %q", password)
	}
}

func TestPromptPasswordUsesTerminalReaderWhenAvailable(t *testing.T) {
	originalReader := stdinReader
	originalFD := stdinFD
	originalIsTerminal := stdinIsTerminal
	originalReadPassword := readPassword
	defer func() {
		stdinReader = originalReader
		stdinFD = originalFD
		stdinIsTerminal = originalIsTerminal
		readPassword = originalReadPassword
	}()

	stdinReader = bufio.NewReader(strings.NewReader("should-not-be-read\n"))
	stdinFD = func() int { return 42 }
	stdinIsTerminal = func(fd int) bool { return fd == 42 }
	readPassword = func(fd int) ([]byte, error) {
		if fd != 42 {
			t.Fatalf("unexpected terminal fd: %d", fd)
		}
		return []byte("terminal-secret"), nil
	}

	password, err := promptPassword("Password: ")
	if err != nil {
		t.Fatalf("promptPassword failed: %v", err)
	}
	if password != "terminal-secret" {
		t.Fatalf("unexpected password: %q", password)
	}
}
