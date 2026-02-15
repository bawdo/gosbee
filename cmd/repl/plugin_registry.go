package main

import "github.com/bawdo/gosbee/plugins"

// pluginEntry represents an enabled plugin in the registry.
type pluginEntry struct {
	name    string                     // "softdelete", "opa"
	factory func() plugins.Transformer // creates fresh instance per manager
	status  func() string              // human-readable status for display
	color   string                     // DOT provenance color
}

// pluginRegistry holds the currently enabled plugins.
type pluginRegistry struct {
	entries []pluginEntry // ordered — plugins apply in registration order
}

// register adds or replaces a plugin by name.
func (r *pluginRegistry) register(entry pluginEntry) {
	for i, e := range r.entries {
		if e.name == entry.name {
			r.entries[i] = entry
			return
		}
	}
	r.entries = append(r.entries, entry)
}

// deregister removes a plugin by name. Returns false if not found.
func (r *pluginRegistry) deregister(name string) bool {
	for i, e := range r.entries {
		if e.name == name {
			r.entries = append(r.entries[:i], r.entries[i+1:]...)
			return true
		}
	}
	return false
}

// deregisterAll removes all plugins.
func (r *pluginRegistry) deregisterAll() {
	r.entries = nil
}

// get looks up a plugin by name.
func (r *pluginRegistry) get(name string) (pluginEntry, bool) {
	for _, e := range r.entries {
		if e.name == name {
			return e, true
		}
	}
	return pluginEntry{}, false
}

// names returns the names of all enabled plugins.
func (r *pluginRegistry) names() []string {
	out := make([]string, len(r.entries))
	for i, e := range r.entries {
		out[i] = e.name
	}
	return out
}

// applyTo calls each plugin's factory and passes the result to the use callback.
func (r *pluginRegistry) applyTo(use func(plugins.Transformer)) {
	for _, entry := range r.entries {
		use(entry.factory())
	}
}

// pluginConfigurer defines a known plugin that can be enabled via the plugin command.
type pluginConfigurer struct {
	name      string
	configure func(s *Session, args string) error
}
