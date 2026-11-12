//go:build !faultinject

package faultinject

// Active reports whether a bug is injected. In the default build it is a
// constant false that the compiler inlines, so no call site survives.
func Active(Switch) bool { return false }
