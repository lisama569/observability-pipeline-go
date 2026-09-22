package pipeline

import (
	"testing"
)

func TestSamplerDeterministic(t *testing.T) {
	s := NewSampler(0.5)
	for _, id := range []uint64{1, 2, 3, 100, 1 << 60} {
		first := s.Decide(id)
		for i := 0; i < 100; i++ {
			if got := s.Decide(id); got != first {
				t.Fatalf("traceID=%d 判定不一致: first=%v then=%v", id, first, got)
			}
		}
		t.Logf("traceID=%d 重复判定 100 次结论一致: %v", id, first)
	}
}

func TestSamplerRateBoundaries(t *testing.T) {
	s0, s1 := NewSampler(0), NewSampler(1)
	for id := uint64(0); id < 1000; id++ {
		if s0.Decide(id) {
			t.Fatal("rate=0 不应采中")
		}
		if !s1.Decide(id) {
			t.Fatal("rate=1 应全部采中")
		}
	}
	// 粗略验证比例在合理区间
	s := NewSampler(0.3)
	hit := 0
	for id := uint64(0); id < 10000; id++ {
		if s.Decide(id) {
			hit++
		}
	}
	t.Logf("rate=0.3 实际命中=%d/10000", hit)
	if hit < 2500 || hit > 3500 {
		t.Errorf("命中率 %d 偏离预期", hit)
	}
}

func TestCacheBoundedFIFO(t *testing.T) {
	c := NewCache(3)
	for i := uint64(1); i <= 5; i++ {
		c.Keep(i)
		t.Logf("keep(%d) 后 len=%d", i, c.Len())
	}
	if c.Len() != 3 {
		t.Fatalf("len=%d, want 3（容量上限）", c.Len())
	}
	if c.Contains(1) || c.Contains(2) {
		t.Error("最旧的 1、2 应被 FIFO 淘汰")
	}
	for _, id := range []uint64{3, 4, 5} {
		if !c.Contains(id) {
			t.Errorf("%d 应被保留", id)
		}
	}
	// 重复保留不占容量
	c.Keep(5)
	if c.Len() != 3 {
		t.Errorf("重复保留后 len=%d, want 3", c.Len())
	}
}

func TestCacheZeroCapacity(t *testing.T) {
	c := NewCache(0)
	c.Keep(1)
	if c.Len() != 0 {
		t.Errorf("容量 0 不应保留任何链路, len=%d", c.Len())
	}
}
