package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"bares3-server/internal/app"
	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/logx"
	"go.uber.org/zap"
	"golang.org/x/term"
)

var stdinReader = bufio.NewReader(os.Stdin)
var stdinFD = func() int { return int(os.Stdin.Fd()) }
var stdinIsTerminal = func(fd int) bool { return term.IsTerminal(fd) }
var readPassword = func(fd int) ([]byte, error) { return term.ReadPassword(fd) }
var stdout io.Writer = os.Stdout
var stderr io.Writer = os.Stderr

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printHelp(stderr)
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		printHelp(stdout)
		return 0
	case "version", "-version", "--version":
		fmt.Fprintln(stdout, buildinfo.Current().String())
		return 0
	case "serve":
		return runServeCommand(args[1:])
	case "init":
		return runInitCommand(args[1:])
	case "resetpassword":
		return runResetPasswordCommand(args[1:])
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printHelp(stderr)
		return 1
	}
}

func printHelp(w io.Writer) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, "%s\n\n", buildinfo.Current().String())
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  bares3d serve [--config path]")
	fmt.Fprintln(w, "  bares3d init [--config path] [--force]")
	fmt.Fprintln(w, "  bares3d resetpassword [--config path]")
	fmt.Fprintln(w, "  bares3d version")
	fmt.Fprintln(w, "  bares3d help")
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func runServeCommand(args []string) int {
	fs := newFlagSet("serve")
	configPath := fs.String("config", "", "path to config.yml")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments for serve: %s\n", strings.Join(fs.Args(), " "))
		return 1
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "failed to load config: %v\n", err)
		return 1
	}

	logger, err := logx.New(logx.Options{
		Name:         "bares3",
		Level:        cfg.Logging.Level,
		Format:       cfg.Logging.Format,
		Dir:          cfg.Paths.LogDir,
		RotateSizeMB: cfg.Logging.RotateSizeMB,
		RotateKeep:   cfg.Logging.RotateKeep,
	})
	if err != nil {
		fmt.Fprintf(stderr, "failed to initialize logger: %v\n", err)
		return 1
	}
	defer func() {
		_ = logger.Sync()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting BareS3", logx.StartField(), zap.String("version", buildinfo.Current().String()))

	service := app.New(cfg, logger)
	if err := service.Run(ctx); err != nil {
		logger.Error("service stopped with error", zap.Error(err))
		return 1
	}
	return 0
}

func runInitCommand(args []string) int {
	fs := newFlagSet("init")
	configPath := fs.String("config", "", "path to config.yml")
	force := fs.Bool("force", false, "overwrite existing console password hash")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments for init: %s\n", strings.Join(fs.Args(), " "))
		return 1
	}

	cfg, path, _, err := config.LoadEditable(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "failed to load editable config: %v\n", err)
		return 1
	}
	if strings.TrimSpace(cfg.Auth.Console.PasswordHash) != "" && !*force {
		fmt.Fprintf(stderr, "console password hash already exists in %s; use --force to replace it\n", path)
		return 1
	}

	if err := runInitWizard(&cfg); err != nil {
		fmt.Fprintf(stderr, "failed to initialize config: %v\n", err)
		return 1
	}
	if err := config.Save(path, cfg); err != nil {
		fmt.Fprintf(stderr, "failed to write config: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "initialized BareS3 config in %s\n", path)
	return 0
}

func runResetPasswordCommand(args []string) int {
	fs := newFlagSet("resetpassword")
	configPath := fs.String("config", "", "path to config.yml")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments for resetpassword: %s\n", strings.Join(fs.Args(), " "))
		return 1
	}

	cfg, path, _, err := config.LoadEditable(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "failed to load editable config: %v\n", err)
		return 1
	}
	if strings.TrimSpace(cfg.Auth.Console.Username) == "" {
		cfg.Auth.Console.Username = "admin"
	}

	password, err := promptPasswordTwice("New console password")
	if err != nil {
		fmt.Fprintf(stderr, "failed to read password: %v\n", err)
		return 1
	}
	hash, err := consoleauth.HashPassword(password)
	if err != nil {
		fmt.Fprintf(stderr, "failed to hash password: %v\n", err)
		return 1
	}
	if strings.TrimSpace(cfg.Auth.Console.SessionSecret) == "" {
		secret, err := consoleauth.GenerateSessionSecret()
		if err != nil {
			fmt.Fprintf(stderr, "failed to generate session secret: %v\n", err)
			return 1
		}
		cfg.Auth.Console.SessionSecret = secret
	}
	if cfg.Auth.Console.SessionTTLMinutes <= 0 {
		cfg.Auth.Console.SessionTTLMinutes = 7 * 24 * 60
	}
	cfg.Auth.Console.PasswordHash = hash

	if err := config.Save(path, cfg); err != nil {
		fmt.Fprintf(stderr, "failed to write config: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "updated console password in %s for user %s\n", path, cfg.Auth.Console.Username)
	return 0
}

func runInitWizard(cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("config is required")
	}

	if err := applyWizardString("Admin listen address", "127.0.0.1:19080", &cfg.Listen.Admin); err != nil {
		return err
	}
	if err := applyWizardString("S3 listen address", "0.0.0.0:9000", &cfg.Listen.S3); err != nil {
		return err
	}
	if err := applyWizardString("File listen address", "0.0.0.0:9001", &cfg.Listen.File); err != nil {
		return err
	}
	if err := applyWizardString("Console username", "admin", &cfg.Auth.Console.Username); err != nil {
		return err
	}

	password, err := promptPasswordTwice("Console password")
	if err != nil {
		return err
	}
	hash, err := consoleauth.HashPassword(password)
	if err != nil {
		return fmt.Errorf("hash console password: %w", err)
	}
	secret, err := consoleauth.GenerateSessionSecret()
	if err != nil {
		return fmt.Errorf("generate session secret: %w", err)
	}

	if cfg.Auth.Console.SessionTTLMinutes <= 0 {
		cfg.Auth.Console.SessionTTLMinutes = 7 * 24 * 60
	}
	cfg.Auth.Console.PasswordHash = hash
	cfg.Auth.Console.SessionSecret = secret
	return nil
}

func applyWizardString(label, fallback string, target *string) error {
	current := strings.TrimSpace(fallback)
	if target != nil && strings.TrimSpace(*target) != "" {
		current = strings.TrimSpace(*target)
	}
	value, err := promptLine(label, current)
	if err != nil {
		return err
	}
	if target != nil {
		*target = value
	}
	return nil
}

func promptLine(label, fallback string) (string, error) {
	if strings.TrimSpace(fallback) != "" {
		fmt.Fprintf(stderr, "%s [%s]: ", label, fallback)
	} else {
		fmt.Fprintf(stderr, "%s: ", label)
	}
	value, err := stdinReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return strings.TrimSpace(fallback), nil
	}
	return trimmed, nil
}

func promptPasswordTwice(label string) (string, error) {
	first, err := promptPassword(label + ": ")
	if err != nil {
		return "", err
	}
	second, err := promptPassword(label + " (again): ")
	if err != nil {
		return "", err
	}
	if first != second {
		return "", fmt.Errorf("passwords do not match")
	}
	if first == "" {
		return "", fmt.Errorf("password must not be empty")
	}
	return first, nil
}

func promptPassword(prompt string) (string, error) {
	fmt.Fprint(stderr, prompt)
	fd := stdinFD()
	if stdinIsTerminal(fd) {
		password, err := readPassword(fd)
		fmt.Fprintln(stderr)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(password)), nil
	}
	password, err := stdinReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(password), nil
}
