package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bawdo/gosbee/managers"
	"github.com/bawdo/gosbee/nodes"
	"github.com/bawdo/gosbee/plugins"
)

// --- DML command handlers ---

func (s *Session) cmdInsertInto(args string) error {
	name := strings.TrimSpace(args)
	if name == "" {
		return errors.New("usage: insert into <table>")
	}
	table := s.resolveTable(name)
	s.setMode(modeInsert)
	s.insertQuery = managers.NewInsertManager(table)
	s.plugins.applyTo(func(t plugins.Transformer) { s.insertQuery.Use(t) })
	_, _ = fmt.Fprintf(s.out, "  INSERT INTO %q\n", name)
	return nil
}

func (s *Session) cmdColumns(args string) error {
	if s.mode != modeInsert || s.insertQuery == nil {
		return errors.New("columns command requires an active INSERT (use 'insert into <table>' first)")
	}
	parts := splitTopLevelCommas(args)
	var cols []nodes.Node
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		col, err := s.resolveColRef(p)
		if err != nil {
			return err
		}
		cols = append(cols, col)
	}
	s.insertQuery.Columns(cols...)
	_, _ = fmt.Fprintf(s.out, "  Columns set (%d)\n", len(cols))
	return nil
}

func (s *Session) cmdValues(args string) error {
	if s.mode != modeInsert || s.insertQuery == nil {
		return errors.New("values command requires an active INSERT (use 'insert into <table>' first)")
	}
	parts := splitTopLevelCommas(args)
	var vals []any
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := parseValue(p)
		if err != nil {
			return fmt.Errorf("values: %w", err)
		}
		vals = append(vals, v)
	}
	s.insertQuery.Values(vals...)
	_, _ = fmt.Fprintf(s.out, "  Values row added (%d values)\n", len(vals))
	return nil
}

func (s *Session) cmdOnConflict(args string) error {
	if s.mode != modeInsert || s.insertQuery == nil {
		return errors.New("on conflict command requires an active INSERT (use 'insert into <table>' first)")
	}
	args = strings.TrimSpace(args)
	// Parse conflict target columns: (col1, col2)
	if !strings.HasPrefix(args, "(") {
		return errors.New("usage: on conflict (<cols>) do nothing | on conflict (<cols>) do update set <col> = <val>")
	}
	closeParen := strings.Index(args, ")")
	if closeParen < 0 {
		return errors.New("missing closing parenthesis in conflict target")
	}
	colsStr := args[1:closeParen]
	rest := strings.TrimSpace(args[closeParen+1:])

	// Parse conflict target columns
	colParts := strings.Split(colsStr, ",")
	var cols []nodes.Node
	for _, p := range colParts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		col, err := s.resolveColRef(p)
		if err != nil {
			return err
		}
		cols = append(cols, col)
	}

	lower := strings.ToLower(rest)
	if lower == "do nothing" {
		s.insertQuery.OnConflict(cols...).DoNothing()
		_, _ = fmt.Fprintln(s.out, "  ON CONFLICT DO NOTHING set")
		return nil
	}
	if strings.HasPrefix(lower, "do update set ") {
		setStr := rest[len("do update set "):]
		tokens := tokenize(setStr)
		if len(tokens) < 3 || tokens[1] != "=" {
			return errors.New("usage: on conflict (<cols>) do update set <col> = <val>")
		}
		col, err := s.resolveColRef(tokens[0])
		if err != nil {
			return err
		}
		val, err := parseValue(tokens[2])
		if err != nil {
			return err
		}
		assignment := &nodes.AssignmentNode{Left: col, Right: nodes.Literal(val)}
		s.insertQuery.OnConflict(cols...).DoUpdate(assignment)
		_, _ = fmt.Fprintln(s.out, "  ON CONFLICT DO UPDATE set")
		return nil
	}
	return errors.New("usage: on conflict (<cols>) do nothing | on conflict (<cols>) do update set <col> = <val>")
}

func (s *Session) cmdUpdate(args string) error {
	name := strings.TrimSpace(args)
	if name == "" {
		return errors.New("usage: update <table>")
	}
	table := s.resolveTable(name)
	s.setMode(modeUpdate)
	s.updateQuery = managers.NewUpdateManager(table)
	s.plugins.applyTo(func(t plugins.Transformer) { s.updateQuery.Use(t) })
	_, _ = fmt.Fprintf(s.out, "  UPDATE %q\n", name)
	return nil
}

func (s *Session) cmdSet(args string) error {
	if s.mode != modeUpdate || s.updateQuery == nil {
		return errors.New("set command requires an active UPDATE (use 'update <table>' first)")
	}
	tokens := tokenize(strings.TrimSpace(args))
	if len(tokens) < 3 || tokens[1] != "=" {
		return errors.New("usage: set <table.col> = <value>")
	}
	col, err := s.resolveColRef(tokens[0])
	if err != nil {
		return err
	}
	val, err := parseValue(tokens[2])
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}
	s.updateQuery.Set(col, val)
	_, _ = fmt.Fprintf(s.out, "  SET %s = %v\n", tokens[0], tokens[2])
	return nil
}

func (s *Session) cmdDeleteFrom(args string) error {
	name := strings.TrimSpace(args)
	if name == "" {
		return errors.New("usage: delete from <table>")
	}
	table := s.resolveTable(name)
	s.setMode(modeDelete)
	s.deleteQuery = managers.NewDeleteManager(table)
	s.plugins.applyTo(func(t plugins.Transformer) { s.deleteQuery.Use(t) })
	_, _ = fmt.Fprintf(s.out, "  DELETE FROM %q\n", name)
	return nil
}

func (s *Session) cmdReturning(args string) error {
	parts := splitTopLevelCommas(args)
	var cols []nodes.Node
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		col, err := s.resolveColRef(p)
		if err != nil {
			return err
		}
		cols = append(cols, col)
	}
	switch s.mode {
	case modeInsert:
		if s.insertQuery == nil {
			return errors.New("no INSERT query defined")
		}
		s.insertQuery.Returning(cols...)
	case modeUpdate:
		if s.updateQuery == nil {
			return errors.New("no UPDATE query defined")
		}
		s.updateQuery.Returning(cols...)
	case modeDelete:
		if s.deleteQuery == nil {
			return errors.New("no DELETE query defined")
		}
		s.deleteQuery.Returning(cols...)
	default:
		return errors.New("returning command requires INSERT, UPDATE, or DELETE mode")
	}
	_, _ = fmt.Fprintf(s.out, "  RETURNING set (%d columns)\n", len(cols))
	return nil
}

// --- DML AST display helpers ---

func (s *Session) cmdASTInsert() error {
	if s.insertQuery == nil {
		return errors.New("no INSERT query defined")
	}
	st := s.insertQuery.Statement
	_, _ = fmt.Fprintf(s.out, "  Engine: %s\n", s.engine)
	_, _ = fmt.Fprintf(s.out, "  Mode: INSERT\n")
	if st.Into != nil {
		_, _ = fmt.Fprintf(s.out, "  INTO:   %s\n", nodeSummary(st.Into))
	}
	if len(st.Columns) > 0 {
		names := make([]string, len(st.Columns))
		for i, c := range st.Columns {
			names[i] = nodeSummary(c)
		}
		_, _ = fmt.Fprintf(s.out, "  COLUMNS: %s\n", strings.Join(names, ", "))
	}
	for i, row := range st.Values {
		names := make([]string, len(row))
		for j, v := range row {
			names[j] = nodeSummary(v)
		}
		_, _ = fmt.Fprintf(s.out, "  VALUES[%d]: %s\n", i, strings.Join(names, ", "))
	}
	if st.OnConflict != nil {
		action := "DO NOTHING"
		if st.OnConflict.Action == nodes.DoUpdate {
			action = "DO UPDATE"
		}
		_, _ = fmt.Fprintf(s.out, "  ON CONFLICT: %s\n", action)
	}
	if len(st.Returning) > 0 {
		names := make([]string, len(st.Returning))
		for i, c := range st.Returning {
			names[i] = nodeSummary(c)
		}
		_, _ = fmt.Fprintf(s.out, "  RETURNING: %s\n", strings.Join(names, ", "))
	}
	return nil
}

func (s *Session) cmdASTUpdate() error {
	if s.updateQuery == nil {
		return errors.New("no UPDATE query defined")
	}
	st := s.updateQuery.Statement
	_, _ = fmt.Fprintf(s.out, "  Engine: %s\n", s.engine)
	_, _ = fmt.Fprintf(s.out, "  Mode: UPDATE\n")
	if st.Table != nil {
		_, _ = fmt.Fprintf(s.out, "  TABLE:  %s\n", nodeSummary(st.Table))
	}
	for i, a := range st.Assignments {
		_, _ = fmt.Fprintf(s.out, "  SET[%d]: %s = %s\n", i, nodeSummary(a.Left), nodeSummary(a.Right))
	}
	if len(st.Wheres) > 0 {
		_, _ = fmt.Fprintf(s.out, "  WHERE:  %d condition(s)\n", len(st.Wheres))
	}
	if len(st.Returning) > 0 {
		names := make([]string, len(st.Returning))
		for i, c := range st.Returning {
			names[i] = nodeSummary(c)
		}
		_, _ = fmt.Fprintf(s.out, "  RETURNING: %s\n", strings.Join(names, ", "))
	}
	return nil
}

func (s *Session) cmdASTDelete() error {
	if s.deleteQuery == nil {
		return errors.New("no DELETE query defined")
	}
	st := s.deleteQuery.Statement
	_, _ = fmt.Fprintf(s.out, "  Engine: %s\n", s.engine)
	_, _ = fmt.Fprintf(s.out, "  Mode: DELETE\n")
	if st.From != nil {
		_, _ = fmt.Fprintf(s.out, "  FROM:   %s\n", nodeSummary(st.From))
	}
	if len(st.Wheres) > 0 {
		_, _ = fmt.Fprintf(s.out, "  WHERE:  %d condition(s)\n", len(st.Wheres))
	}
	if len(st.Returning) > 0 {
		names := make([]string, len(st.Returning))
		for i, c := range st.Returning {
			names[i] = nodeSummary(c)
		}
		_, _ = fmt.Fprintf(s.out, "  RETURNING: %s\n", strings.Join(names, ", "))
	}
	return nil
}
