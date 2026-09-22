// Package errkind classifies errors from a request lifecycle into
// distinguishable categories while preserving the underlying cause.
//
// Categories:
//
//   - KindBusiness: a caller-visible business failure. Its message is safe
//     to surface verbatim.
//   - KindCanceled: the request/context was canceled. Marks a terminated chain.
//   - KindDeadlineExceeded: a deadline elapsed. Marks a terminated chain.
//   - KindInternal: any other failure. Internal details are never surfaced
//     verbatim; the original error is preserved via Unwrap for diagnostics.
package errkind

import (
	"context"
	"errors"
	"fmt"
)

// Kind is the classified category of an error.
type Kind int

const (
	// KindNone means no error was observed.
	KindNone Kind = iota
	// KindBusiness is a caller-visible business error (safe to surface).
	KindBusiness
	// KindCanceled means the context (or request) was canceled.
	KindCanceled
	// KindDeadlineExceeded means the context deadline was exceeded.
	KindDeadlineExceeded
	// KindInternal is an internal failure whose details must not leak verbatim.
	KindInternal
)

// businessError marks an error as a caller-visible business failure.
type businessError struct {
	err error
}

func (e *businessError) Error() string { return e.err.Error() }
func (e *businessError) Unwrap() error { return e.err }

// Class is the result of classifying an error. It implements error with a
// sanitized message while keeping the original cause reachable via Unwrap.
type Class struct {
	kind Kind
	err  error
}

// stable, sanitized outward-facing messages per category.
const (
	msgCanceled = "request canceled"
	msgTimeout  = "request timed out"
	msgInternal = "internal error"
)

// Error returns a stable, sanitized message for the class. Internal errors
// never expose their underlying message here.
func (c Class) Error() string {
	switch c.kind {
	case KindBusiness:
		if c.err != nil {
			return c.err.Error()
		}
		return "business error"
	case KindCanceled:
		return msgCanceled
	case KindDeadlineExceeded:
		return msgTimeout
	case KindInternal:
		return msgInternal
	default:
		return ""
	}
}

// Kind returns the classified category.
func (c Class) Kind() Kind { return c.kind }

// Unwrap returns the preserved underlying cause.
func (c Class) Unwrap() error { return c.err }

// Cause returns the preserved underlying error.
func (c Class) Cause() error { return c.err }

// Terminated reports whether the error marks a terminated chain
// (canceled or deadline exceeded).
func (c Class) Terminated() bool {
	return c.kind == KindCanceled || c.kind == KindDeadlineExceeded
}

// String returns a stable machine-readable name for the category.
func (k Kind) String() string {
	switch k {
	case KindBusiness:
		return "business"
	case KindCanceled:
		return "canceled"
	case KindDeadlineExceeded:
		return "deadline_exceeded"
	case KindInternal:
		return "internal"
	default:
		return "none"
	}
}

// Business wraps an error as a caller-visible business error. A nil error
// yields a KindNone class.
func Business(err error) Class {
	if err == nil {
		return Class{}
	}
	return Class{kind: KindBusiness, err: &businessError{err: err}}
}

// Businessf wraps a formatted business error.
func Businessf(format string, args ...any) Class {
	return Business(fmt.Errorf(format, args...))
}

// Classify categorizes an error. Deadline is tested before cancellation.
// Unknown/non-nil errors are treated as internal so implementation details
// never leak as a business conclusion. A nil error yields KindNone.
func Classify(err error) Class {
	if err == nil {
		return Class{}
	}
	// Preserve an already-classified error exactly.
	var already Class
	if errors.As(err, &already) {
		return already
	}
	// Deadline before cancellation: a deadline is not a cancellation, but
	// wrappers may embed either; deadline is the more specific conclusion.
	if errors.Is(err, context.DeadlineExceeded) {
		return Class{kind: KindDeadlineExceeded, err: err}
	}
	if errors.Is(err, context.Canceled) {
		return Class{kind: KindCanceled, err: err}
	}
	var be *businessError
	if errors.As(err, &be) {
		return Class{kind: KindBusiness, err: be}
	}
	return Class{kind: KindInternal, err: err}
}

// Sanitize converts an error into a safe outward-facing error. The returned
// error renders only a sanitized message; the original cause remains
// reachable via errors.Is/errors.As and Unwrap for internal diagnostics.
// nil maps to nil.
func Sanitize(err error) error {
	if err == nil {
		return nil
	}
	c := Classify(err)
	if c.kind == KindNone {
		return nil
	}
	return c
}
