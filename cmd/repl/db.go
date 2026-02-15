package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

var driverName = map[string]string{
	"postgres": "pgx",
	"mysql":    "mysql",
	"sqlite":   "sqlite",
}

const maxRows = 1000

type schemaCache struct {
	tables  []string
	columns map[string][]string // table name -> column names
}

type dbConn struct {
	db     *sql.DB
	dsn    string
	engine string
	schema schemaCache
}

func connect(engine, dsn string) (*dbConn, error) {
	driver, ok := driverName[engine]
	if !ok {
		return nil, fmt.Errorf("no driver for engine %q", engine)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	conn := &dbConn{db: db, dsn: dsn, engine: engine}
	conn.schema.columns = make(map[string][]string)
	if err := conn.loadSchema(); err != nil {
		// Non-fatal: schema introspection is best-effort for autocomplete.
		fmt.Fprintf(os.Stderr, "  Note: schema introspection failed: %v\n", err)
	}
	return conn, nil
}

func (c *dbConn) close() error {
	return c.db.Close()
}

func (c *dbConn) execQuery(sqlStr string, params []any) (string, error) {
	rows, err := c.db.Query(sqlStr, params...)
	if err != nil {
		return "", fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return formatRows(rows)
}

func formatRows(rows *sql.Rows) (string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("columns: %w", err)
	}

	var data [][]string
	truncated := false
	for rows.Next() {
		if len(data) >= maxRows {
			truncated = true
			break
		}
		vals := make([]*sql.NullString, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			vals[i] = &sql.NullString{}
			ptrs[i] = vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", fmt.Errorf("scan: %w", err)
		}
		row := make([]string, len(columns))
		for i, v := range vals {
			if v.Valid {
				row[i] = v.String
			} else {
				row[i] = "NULL"
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("rows: %w", err)
	}

	result := formatTable(columns, data)
	if truncated {
		result += fmt.Sprintf("(truncated at %d rows)\n", maxRows)
	}
	return result, nil
}

func formatTable(columns []string, rows [][]string) string {
	if len(columns) == 0 {
		return "(0 rows)\n"
	}

	// Calculate column widths.
	widths := make([]int, len(columns))
	for i, c := range columns {
		widths[i] = len(c)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	var b strings.Builder

	// Separator line.
	sep := buildSeparator(widths)

	b.WriteString(sep)
	// Header.
	b.WriteByte('|')
	for i, c := range columns {
		fmt.Fprintf(&b, " %-*s |", widths[i], c)
	}
	b.WriteByte('\n')
	b.WriteString(sep)

	// Data rows.
	for _, row := range rows {
		b.WriteByte('|')
		for i, cell := range row {
			fmt.Fprintf(&b, " %-*s |", widths[i], cell)
		}
		b.WriteByte('\n')
	}

	b.WriteString(sep)

	// Row count.
	n := len(rows)
	if n == 1 {
		b.WriteString("(1 row)\n")
	} else {
		fmt.Fprintf(&b, "(%d rows)\n", n)
	}

	return b.String()
}

func buildSeparator(widths []int) string {
	var b strings.Builder
	b.WriteByte('+')
	for _, w := range widths {
		for j := 0; j < w+2; j++ {
			b.WriteByte('-')
		}
		b.WriteByte('+')
	}
	b.WriteByte('\n')
	return b.String()
}

func (c *dbConn) loadSchema() error {
	var query string
	switch c.engine {
	case "postgres":
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
	case "mysql":
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name"
	case "sqlite":
		query = "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	default:
		return fmt.Errorf("unsupported engine: %s", c.engine)
	}
	tables, err := c.queryStringColumn(query)
	if err != nil {
		return err
	}
	c.schema.tables = tables
	return nil
}

func (c *dbConn) schemaTables() []string {
	return c.schema.tables
}

func (c *dbConn) schemaColumns(table string) []string {
	if cols, ok := c.schema.columns[table]; ok {
		return cols
	}
	var query string
	var param any
	switch c.engine {
	case "postgres":
		query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position"
		param = table
	case "mysql":
		query = "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position"
		param = table
	case "sqlite":
		query = "SELECT name FROM pragma_table_info(?)"
		param = table
	default:
		return nil
	}
	cols, err := c.queryStringColumn(query, param)
	if err != nil {
		return nil
	}
	c.schema.columns[table] = cols
	return cols
}

func (c *dbConn) queryStringColumn(query string, params ...any) ([]string, error) {
	rows, err := c.db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func sanitizeDSN(dsn string) string {
	// Try parsing as URL (postgres style).
	u, err := url.Parse(dsn)
	if err == nil && u.Scheme != "" && u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			// Rebuild manually to avoid percent-encoding the mask.
			masked := u.Scheme + "://" + u.User.Username() + ":****@" + u.Host + u.Path
			if u.RawQuery != "" {
				masked += "?" + u.RawQuery
			}
			return masked
		}
		return dsn
	}

	// Try MySQL-style DSN: user:pass@tcp(host)/db
	if atIdx := strings.Index(dsn, "@"); atIdx > 0 {
		userPass := dsn[:atIdx]
		if colonIdx := strings.Index(userPass, ":"); colonIdx >= 0 {
			return userPass[:colonIdx+1] + "****" + dsn[atIdx:]
		}
	}

	return dsn
}
