package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
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

func main() {
	if len(os.Args) > 1 && os.Args[1] == "auth" {
		os.Exit(runAuthCommand(os.Args[2:]))
	}

	configPath := flag.String("config", "", "path to config.yml")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Fprintln(os.Stdout, buildinfo.Current().String())
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logger.Sync()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting BareS3", logx.StartField(), zap.String("version", buildinfo.Current().String()))

	service := app.New(cfg, logger)
	if err := service.Run(ctx); err != nil {
		logger.Fatal("service stopped with error", zap.Error(err))
	}
}

func runAuthCommand(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: bares3d auth <hash|init>")
		return 1
	}

	switch args[0] {
	case "hash":
		password, err := promptPasswordTwice("New console password")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to read password: %v\n", err)
			return 1
		}
		hash, err := consoleauth.HashPassword(password)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to hash password: %v\n", err)
			return 1
		}
		fmt.Fprintln(os.Stdout, hash)
		return 0
	case "init":
		return runAuthInit(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown auth subcommand %q\n", args[0])
		return 1
	}
}

func runAuthInit(args []string) int {
	fs := flag.NewFlagSet("auth init", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "path to config.yml")
	force := fs.Bool("force", false, "overwrite existing console password hash")
	username := fs.String("username", "", "override console username")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	cfg, path, _, err := config.LoadEditable(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load editable config: %v\n", err)
		return 1
	}

	if strings.TrimSpace(*username) != "" {
		cfg.Auth.Console.Username = strings.TrimSpace(*username)
	}
	if strings.TrimSpace(cfg.Auth.Console.Username) == "" {
		cfg.Auth.Console.Username = "admin"
	}
	if cfg.Auth.Console.SessionTTLMinutes <= 0 {
		cfg.Auth.Console.SessionTTLMinutes = 7 * 24 * 60
	}
	if strings.TrimSpace(cfg.Auth.Console.PasswordHash) != "" && !*force {
		fmt.Fprintf(os.Stderr, "console password hash already exists in %s; use --force to replace it\n", path)
		return 1
	}

	password, err := promptPasswordTwice("Console password")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read password: %v\n", err)
		return 1
	}
	hash, err := consoleauth.HashPassword(password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to hash password: %v\n", err)
		return 1
	}
	secret, err := consoleauth.GenerateSessionSecret()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate session secret: %v\n", err)
		return 1
	}

	cfg.Auth.Console.PasswordHash = hash
	if *force || strings.TrimSpace(cfg.Auth.Console.SessionSecret) == "" {
		cfg.Auth.Console.SessionSecret = secret
	}

	if err := config.Save(path, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write config: %v\n", err)
		return 1
	}

	fmt.Fprintf(os.Stdout, "initialized console auth in %s for user %s\n", path, cfg.Auth.Console.Username)
	return 0
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
	fmt.Fprint(os.Stderr, prompt)
	fd := stdinFD()
	if stdinIsTerminal(fd) {
		password, err := readPassword(fd)
		fmt.Fprintln(os.Stderr)
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
