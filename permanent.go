package pipeline

import (
	"errors"
	"fmt"
)

// PermanentError 包装一个错误，表示“接收端明确拒绝该批次，再投也无用”。
// 管线遇到该错误后立即停止重试，以独立的丢弃原因上报。
type PermanentError struct {
	Cause error
}

func (e *PermanentError) Error() string {
	if e.Cause == nil {
		return "permanent export failure"
	}
	return fmt.Sprintf("permanent export failure: %v", e.Cause)
}

func (e *PermanentError) Unwrap() error { return e.Cause }

// Permanent 把一个错误标记为永久失败；err 为 nil 时返回 nil。
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Cause: err}
}

// permanentFailer 由自定义导出错误实现，以显式声明该错误不可重试。
type permanentFailer interface {
	PermanentFailure()
}

// IsPermanentFailure 判断导出错误是否为永久失败：
// 被 Permanent 包装（errors.Is 匹配），或实现 PermanentFailure() 标记方法。
// 注意：context.Canceled / DeadlineExceeded 不属于永久失败，
// 它们代表取消/超时而非数据被拒，仍按取消路径处理。
func IsPermanentFailure(err error) bool {
	if err == nil {
		return false
	}
	var pe *PermanentError
	if errors.As(err, &pe) {
		return true
	}
	var pf permanentFailer
	return errors.As(err, &pf)
}
