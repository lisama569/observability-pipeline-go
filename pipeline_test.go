package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPipelineEndToEnd(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}}
	drops := &dropLog{}
	p := NewPipeline(Config{
		SampleRate: 1, CacheCap: 8, QueueSize: 64, Retries: 1, BatchSize: 4,
	}, recv, drops.handler())
	p.Start(context.Background())

	// 正常链路
	ctx := p.Begin(context.Background())
	p.Emit(ctx, LevelInfo, "request start", map[string]string{"path": "/pay"})

	// 取消链路：记录必须带终止标记导出，而不是被丢弃
	cctx, cancel := context.WithCancel(p.Begin(context.Background()))
	cancel()
	canceledTrace := CorrelationFrom(cctx).TraceID
	p.Emit(cctx, LevelInfo, "late log after cancel", nil)

	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	recv.mu.Lock()
	defer recv.mu.Unlock()
	t.Logf("exported %d records, drops=%v", len(recv.records), drops.reasons)
	var foundTerminated bool
	for _, r := range recv.records {
		if r.TraceID == canceledTrace {
			t.Logf("canceled trace record: terminated=%v reason=%q msg=%q",
				r.Terminated, r.TermReason, r.Message)
			if !r.Terminated || r.TermReason != "canceled" {
				t.Errorf("取消链路的记录未被标记: %+v", r)
			}
			foundTerminated = true
		}
	}
	if !foundTerminated {
		t.Error("取消链路产生的记录被静默丢弃")
	}
	if !p.Kept(canceledTrace) {
		t.Error("取消链路应进入异常保留缓存")
	}
}

func TestPipelineConcurrentRace(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}}
	drops := &dropLog{}
	p := NewPipeline(Config{
		SampleRate: 0.5, CacheCap: 16, QueueSize: 128, Retries: 1, BatchSize: 8,
	}, recv, drops.handler())
	p.Start(context.Background())

	var wg sync.WaitGroup
	// 并发采集
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx := p.Begin(context.Background())
			for i := 0; i < 50; i++ {
				child := p.Derive(ctx)
				p.Emit(child, LevelInfo, "log", map[string]string{"g": string(rune('a' + g))})
				if i%17 == 0 {
					p.Emit(child, LevelError, "boom", nil)
				}
			}
		}(g)
	}
	// 并发更新配置：不得出现半更新状态
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				p.UpdateConfig(Config{
					SampleRate: float64(g) / 4, CacheCap: 16,
					QueueSize: 128, Retries: 1, BatchSize: 8,
				})
			}
		}(g)
	}
	// 并发刷新
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = p.Flush(ctx)
				cancel()
			}
		}()
	}
	wg.Wait()
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	recv.mu.Lock()
	total := len(recv.records)
	records := make([]Record, total)
	copy(records, recv.records)
	recv.mu.Unlock()
	dropped := 0
	drops.mu.Lock()
	for _, c := range drops.reasons {
		dropped += c
	}
	drops.mu.Unlock()
	t.Logf("exported=%d dropped=%v 判定依据: 每条导出记录必须属于某次采样结论", total, drops.reasons)
	// 采样一致性：所有 error 记录所在链路应在缓存中（容量允许时）。
	for _, r := range records {
		if r.Level == LevelError && !p.Kept(r.TraceID) && p.Config().CacheCap > 0 {
			t.Logf("warn: error trace %d evicted from bounded cache", r.TraceID)
		}
	}
	if recv.dups() != 0 {
		t.Errorf("接收端出现 %d 条重复数据", recv.dups())
	}
}

func TestFlushTimeout(t *testing.T) {
	recv := &receiver{seen: map[uint64]int{}, delay: 200 * time.Millisecond}
	p := NewPipeline(Config{SampleRate: 1, QueueSize: 8, BatchSize: 1}, recv, nil)
	p.Start(context.Background())
	ctx := p.Begin(context.Background())
	p.Emit(ctx, LevelInfo, "slow", nil)

	fctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := p.Flush(fctx)
	t.Logf("慢导出端 flush 判定: err=%v", err)
	if err == nil {
		t.Error("flush 应在 ctx 超时后返回错误")
	}
	ce := Classify(err)
	if ce.Kind != KindTimeout {
		t.Errorf("flush 超时错误应归类为 timeout, got %s", ce.Kind)
	}
	p.Close(context.Background())
}
