package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type bizErr struct{ msg string }

func (e bizErr) Error() string    { return e.msg }
func (e bizErr) IsBusinessError() {}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want ErrorKind
	}{
		{"business", bizErr{"余额不足"}, KindBusiness},
		{"canceled", context.Canceled, KindCanceled},
		{"timeout", context.DeadlineExceeded, KindTimeout},
		{"wrapped_canceled", fmt.Errorf("rpc: %w", context.Canceled), KindCanceled},
		{"wrapped_timeout", fmt.Errorf("db: %w", context.DeadlineExceeded), KindTimeout},
		{"internal", errors.New("null pointer at 0x7f3c"), KindInternal},
	}
	for _, tc := range cases {
		got := Classify(tc.in)
		t.Logf("input=%q kind=%s public=%q", tc.in, got.Kind, got.Public())
		if got.Kind != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got.Kind, tc.want)
		}
		if !errors.Is(got, tc.in) {
			t.Errorf("%s: cause not preserved (errors.Is failed)", tc.name)
		}
	}
}

func TestInternalErrorNotLeaked(t *testing.T) {
	in := errors.New("secret internal detail: /etc/passwd")
	got := Classify(in)
	t.Logf("input=%q kind=%s public=%q", in, got.Kind, got.Public())
	if got.Public() == got.Error() {
		t.Errorf("internal error leaked verbatim: %q", got.Public())
	}
	if got.Public() != "internal" {
		t.Errorf("public=%q, want %q", got.Public(), "internal")
	}
	// 业务错误允许透出原因
	biz := Classify(bizErr{"余额不足"})
	if biz.Public() == "internal" {
		t.Errorf("business error should be visible, got %q", biz.Public())
	}
}
