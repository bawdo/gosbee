// REPL binary for interactively building and executing SQL queries.
//
// Configuration (env vars):
//
//	GOSBEE_ENGINE=postgres|mysql|sqlite  (optional, prompted if absent)
//	DATABASE_URL=<dsn>                    (optional, auto-connects if set)
//
// Usage:
//
//	go run ./cmd/repl
package main

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/ergochat/readline"
)

func main() {
	rl, err := readline.NewFromConfig(&readline.Config{
		Prompt:          "[Config] ",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "readline init: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = rl.Close() }()

	engine := loadEngine(rl)
	sess := NewSession(engine, rl)

	// Set up the completer now that we have a session.
	comp := &replCompleter{sess: sess}
	_ = rl.SetConfig(&readline.Config{
		Prompt:          "gosbee> ",
		HistoryFile:     historyPath(),
		HistoryLimit:    500,
		AutoComplete:    comp,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})

	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		fmt.Printf("[Config] Connecting via DATABASE_URL...\n")
		if err := sess.Execute("connect " + dsn); err != nil {
			fmt.Fprintf(os.Stderr, "  Warning: DATABASE_URL connect failed: %v\n", err)
		}
	} else {
		loadConnection(rl, sess)
	}

	fmt.Println()
	fmt.Println("Gosbee REPL — type 'help' for commands, 'exit' to quit")
	fmt.Println()

	rl.SetPrompt("gosbee> ")
	for {
		line, err := rl.ReadLine()
		if errors.Is(err, readline.ErrInterrupt) {
			continue
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if lower == "exit" || lower == "quit" {
			break
		}
		if err := sess.Execute(line); err != nil {
			fmt.Fprintf(os.Stderr, "  Error: %v\n", err)
		}
	}
	if sess.conn != nil {
		_ = sess.conn.close()
	}
	fmt.Println()
}

func loadEngine(rl *readline.Instance) string {
	engine := strings.TrimSpace(strings.ToLower(os.Getenv("GOSBEE_ENGINE")))
	if engine != "" {
		if !isValidEngine(engine) {
			fmt.Fprintf(os.Stderr, "Warning: invalid GOSBEE_ENGINE=%q, defaulting to postgres\n", engine)
			return "postgres"
		}
		fmt.Printf("[Config] Engine: %s (from GOSBEE_ENGINE)\n", engine)
		return engine
	}

	choice := prompt(rl, "Select engine (postgres, mysql, sqlite)", "postgres")
	choice = strings.TrimSpace(strings.ToLower(choice))
	if choice != "" {
		if !isValidEngine(choice) {
			fmt.Fprintf(os.Stderr, "Warning: unknown engine %q, defaulting to postgres\n", choice)
			return "postgres"
		}
		fmt.Printf("[Config] Engine: %s\n", choice)
		return choice
	}
	fmt.Println("[Config] Engine: postgres")
	return "postgres"
}

func loadConnection(rl *readline.Instance, sess *Session) {
	answer := prompt(rl, "Connect to a database? (y/N)", "")
	answer = strings.TrimSpace(strings.ToLower(answer))
	if answer != "y" && answer != "yes" {
		fmt.Println("[Config] Skipped — use 'connect <dsn>' later to connect")
		return
	}

	var dsn string
	switch sess.engine {
	case "sqlite":
		dsn = buildSQLiteDSN(rl)
	case "mysql":
		dsn = buildMySQLDSN(rl)
	default:
		dsn = buildPostgresDSN(rl)
	}

	if dsn == "" {
		fmt.Println("[Config] No connection configured — use 'connect <dsn>' later")
		return
	}

	fmt.Printf("[Config] DSN: %s\n", sanitizeDSN(dsn))
	if err := sess.Execute("connect " + dsn); err != nil {
		fmt.Fprintf(os.Stderr, "  Warning: connect failed: %v\n", err)
		fmt.Println("[Config] Use 'connect <dsn>' later to retry")
	}
}

// prompt prints a label with an optional default and returns the user's input
// (or the default if they press enter).
func prompt(rl *readline.Instance, label, defaultVal string) string {
	if rl == nil {
		return defaultVal
	}
	if defaultVal != "" {
		rl.SetPrompt(fmt.Sprintf("[Config]   %s [%s]: ", label, defaultVal))
	} else {
		rl.SetPrompt(fmt.Sprintf("[Config]   %s: ", label))
	}
	defer rl.SetPrompt("gosbee> ")
	line, err := rl.ReadLine()
	if err != nil {
		return defaultVal
	}
	val := strings.TrimSpace(line)
	if val == "" {
		return defaultVal
	}
	return val
}

func buildSQLiteDSN(rl *readline.Instance) string {
	fmt.Println("[Config] SQLite connection setup:")
	path := prompt(rl, "Database path", ":memory:")
	return path
}

func buildPostgresDSN(rl *readline.Instance) string {
	fmt.Println("[Config] PostgreSQL connection setup:")

	defaultUser := "postgres"
	if u, err := user.Current(); err == nil && u.Username != "" {
		defaultUser = u.Username
	}

	dbUser := prompt(rl, "User", defaultUser)
	dbPass := prompt(rl, "Password", "")
	host := prompt(rl, "Host", "localhost")
	port := prompt(rl, "Port", "5432")
	dbName := prompt(rl, "Database", dbUser)
	sslMode := prompt(rl, "SSL mode (disable/require/verify-full)", "disable")

	var userInfo *url.Userinfo
	if dbPass != "" {
		userInfo = url.UserPassword(dbUser, dbPass)
	} else {
		userInfo = url.User(dbUser)
	}
	u := &url.URL{
		Scheme:   "postgres",
		User:     userInfo,
		Host:     host + ":" + port,
		Path:     "/" + dbName,
		RawQuery: "sslmode=" + sslMode,
	}
	return u.String()
}

func buildMySQLDSN(rl *readline.Instance) string {
	fmt.Println("[Config] MySQL connection setup:")

	dbUser := prompt(rl, "User", "root")
	dbPass := prompt(rl, "Password", "")
	host := prompt(rl, "Host", "localhost")
	port := prompt(rl, "Port", "3306")
	dbName := prompt(rl, "Database", "")

	if dbName == "" {
		return ""
	}

	// Format: user:pass@tcp(host:port)/dbname
	var auth string
	if dbPass != "" {
		auth = dbUser + ":" + dbPass
	} else {
		auth = dbUser
	}
	return fmt.Sprintf("%s@tcp(%s:%s)/%s", auth, host, port, dbName)
}

func isValidEngine(engine string) bool {
	switch engine {
	case "postgres", "mysql", "sqlite":
		return true
	}
	return false
}

func historyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".gosbee_history")
}
