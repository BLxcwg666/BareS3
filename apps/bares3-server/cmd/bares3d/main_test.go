package main

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"bares3-server/internal/config"
)

func TestRunWithoutArgsPrintsHelp(t *testing.T) {
	stdoutBuffer, stderrBuffer, restore := swapCLIIO(strings.NewReader(""))
	defer restore()

	if code := run(nil); code != 0 {
		t.Fatalf("run() returned %d", code)
	}
	if stdoutBuffer.Len() != 0 {
		t.Fatalf("expected help on stderr for empty args, got stdout=%q", stdoutBuffer.String())
	}
	if !strings.Contains(stderrBuffer.String(), "bares3d serve") {
		t.Fatalf("expected help output, got %q", stderrBuffer.String())
	}
}

func TestHelpCommandPrintsHelp(t *testing.T) {
	stdoutBuffer, _, restore := swapCLIIO(strings.NewReader(""))
	defer restore()

	if code := run([]string{"help"}); code != 0 {
		t.Fatalf("run(help) returned %d", code)
	}
	if !strings.Contains(stdoutBuffer.String(), "bares3d init") {
		t.Fatalf("expected help output, got %q", stdoutBuffer.String())
	}
}

func TestInitCommandWritesConfiguredValues(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yml")
	input := strings.Join([]string{
		"127.0.0.1:29080",
		"0.0.0.0:9900",
		"0.0.0.0:9901",
		"ops-admin",
		"secret-password",
		"secret-password",
		"",
	}, "\n")
	_, _, restore := swapCLIIO(strings.NewReader(input))
	defer restore()

	if code := run([]string{"init", "--config", configPath}); code != 0 {
		t.Fatalf("run(init) returned %d", code)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Listen.Admin != "127.0.0.1:29080" || cfg.Listen.S3 != "0.0.0.0:9900" || cfg.Listen.File != "0.0.0.0:9901" {
		t.Fatalf("unexpected listen config: %+v", cfg.Listen)
	}
	if cfg.App.Env != "production" {
		t.Fatalf("unexpected app env: %q", cfg.App.Env)
	}
	if cfg.Auth.Console.Username != "ops-admin" {
		t.Fatalf("unexpected username: %q", cfg.Auth.Console.Username)
	}
	if strings.TrimSpace(cfg.Auth.Console.PasswordHash) == "" || strings.TrimSpace(cfg.Auth.Console.SessionSecret) == "" {
		t.Fatalf("expected password hash and session secret to be set: %+v", cfg.Auth.Console)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	text := string(content)
	if strings.Contains(text, "\nsync:\n") || strings.Contains(text, "access_key_id:") {
		t.Fatalf("expected deprecated config blocks to stay omitted, got:\n%s", text)
	}
}

func TestResetPasswordCommandUpdatesExistingConfig(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config.yml")
	cfg := config.Default()
	cfg.Auth.Console.Username = "admin"
	cfg.Auth.Console.PasswordHash = "old-hash"
	cfg.Auth.Console.SessionSecret = "existing-session-secret"
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	_, _, restore := swapCLIIO(strings.NewReader("new-password\nnew-password\n"))
	defer restore()

	if code := run([]string{"resetpassword", "--config", configPath}); code != 0 {
		t.Fatalf("run(resetpassword) returned %d", code)
	}

	updated, err := config.Load(configPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if updated.Auth.Console.PasswordHash == "old-hash" {
		t.Fatalf("expected password hash to change")
	}
	if updated.Auth.Console.SessionSecret != "existing-session-secret" {
		t.Fatalf("expected existing session secret to be preserved, got %q", updated.Auth.Console.SessionSecret)
	}
}

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

func swapCLIIO(input *strings.Reader) (*bytes.Buffer, *bytes.Buffer, func()) {
	stdoutBuffer := new(bytes.Buffer)
	stderrBuffer := new(bytes.Buffer)
	originalStdout := stdout
	originalStderr := stderr
	originalReader := stdinReader
	stdout = stdoutBuffer
	stderr = stderrBuffer
	stdinReader = bufio.NewReader(input)
	return stdoutBuffer, stderrBuffer, func() {
		stdout = originalStdout
		stderr = originalStderr
		stdinReader = originalReader
	}
}
