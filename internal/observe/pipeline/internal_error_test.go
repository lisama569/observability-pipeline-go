package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lisama569/observability-pipeline-go/internal/observe/export"
	"github.com/lisama569/observability-pipeline-go/internal/observe/wire"
)

// Internal errors must reach the export as category "internal" without their
// raw message becoming the outward conclusion; the chain itself is still
// retained (exceptional) and exportable.
func TestInternalErrorDoesNotLeakButChainRetained(t *testing.T) {
	exp := &recExporter{}
	p := New(exp, baseCfg(0), nil)
	ctx := context.Background()
	p.Start(ctx)

	internal := errors.New("segfault in /internal/store/shard.go:7")
	c, sp := p.StartSpan(ctx, "op")
	sp.End(c, internal)
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	batches := exp.records()
	if len(batches) != 1 || len(batches[0].Records) != 1 {
		t.Fatalf("exceptional internal chain must still be exported: %+v", batches)
	}
	r := batches[0].Records[0]
	t.Logf("input=%q => exported error_kind=%q; diagnostic field retains raw detail=%v; outward message leaks detail=%v (basis: kind recorded, conclusion sanitized to stable category)",
		internal.Error(), r.ErrorKind, bytesContainsRaw(&batches[0]), strings.Contains(r.Message, "segfault"))
	if r.ErrorKind != "internal" {
		t.Fatalf("want internal category, got %q", r.ErrorKind)
	}
	// The outward conclusion (message + machine-facing kind) must not carry
	// the internal detail; raw cause stays only in the internal diagnostic
	// field so it remains traceable without becoming the public conclusion.
	if strings.Contains(r.Message, "segfault") {
		t.Fatalf("internal detail leaked into the record message: %q", r.Message)
	}
	_ = p.Shutdown(ctx)
}

func bytesContainsRaw(b *wire.Batch) bool {
	for _, r := range b.Records {
		if v, ok := r.Fields["error"]; ok {
			if strings.Contains(v.(string), "segfault") {
				return true
			}
		}
	}
	return false
}

var _ = export.ReasonQueueFull
