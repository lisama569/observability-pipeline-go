package errkind

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want Kind
		term bool
	}{
		{"nil", nil, KindNone, false},
		{"business", Businessf("user %d not found", 7), KindBusiness, false},
		{"canceled", context.Canceled, KindCanceled, true},
		{"deadline", context.DeadlineExceeded, KindDeadlineExceeded, true},
		{"wrapped canceled", fmt.Errorf("layer: %w", context.Canceled), KindCanceled, true},
		{"wrapped deadline", fmt.Errorf("layer: %w", context.DeadlineExceeded), KindDeadlineExceeded, true},
		{"raw unknown", errors.New("db down"), KindInternal, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Classify(tc.in)
			t.Logf("input=%v => kind=%s terminated=%v (basis: errors.Is over canceled/deadline, business marker, else internal)",
				tc.in, c.Kind(), c.Terminated())
			if c.Kind() != tc.want {
				t.Fatalf("kind: got %s want %s", c.Kind(), tc.want)
			}
			if c.Terminated() != tc.term {
				t.Fatalf("terminated: got %v want %v", c.Terminated(), tc.term)
			}
			// Underlying cause must remain traceable.
			if tc.in != nil && c.Cause() == nil {
				t.Fatalf("underlying cause was not preserved")
			}
		})
	}
}

func TestClassifyIdempotent(t *testing.T) {
	first := Classify(errors.New("boom"))
	second := Classify(first)
	t.Logf("input=%q classify twice => %s (basis: errors.As(errkind.Class) preserves category)",
		first.Error(), second.Kind())
	if first.Kind() != second.Kind() || first.Kind() != KindInternal {
		t.Fatalf("reclassification changed kind: %s -> %s", first.Kind(), second.Kind())
	}
}

func TestSanitizeHidesInternal(t *testing.T) {
	secret := errors.New("panic: nil deref at internal/db.go:42")
	out := Sanitize(secret)
	t.Logf("input=%q => outward=%q (basis: internal errors render stable generic message)",
		secret.Error(), out.Error())
	if out.Error() == secret.Error() {
		t.Fatalf("internal details leaked outward: %v", out)
	}
	// Cause stays traceable internally.
	if !errors.Is(out, secret) {
		t.Fatalf("sanitized error lost the original cause via errors.Is")
	}

	// Business errors keep their message.
	biz := Businessf("quota exceeded")
	if got := Sanitize(biz).Error(); got != "quota exceeded" {
		t.Fatalf("business message changed: %q", got)
	}
	t.Logf("business input=%q => outward=%q (basis: business category is caller-safe)",
		biz.Error(), Sanitize(biz).Error())

	if Sanitize(nil) != nil {
		t.Fatalf("nil must sanitize to nil")
	}
}

func TestKindStableNames(t *testing.T) {
	for _, k := range []Kind{KindBusiness, KindCanceled, KindDeadlineExceeded, KindInternal, KindNone} {
		t.Logf("kind=%d name=%q", k, k.String())
		if k.String() == "" {
			t.Fatalf("empty name for %d", k)
		}
	}
}
