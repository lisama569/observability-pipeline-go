package pipeline

import (
	"context"
	"errors"
	"fmt"
)

// ErrorKind 对链路生命周期中的错误进行归类。
type ErrorKind int

const (
	KindBusiness ErrorKind = iota
	KindCanceled
	KindTimeout
	KindInternal
)

func (k ErrorKind) String() string {
	switch k {
	case KindBusiness:
		return "business"
	case KindCanceled:
		return "canceled"
	case KindTimeout:
		return "timeout"
	default:
		return "internal"
	}
}

// Error 保留底层原因并携带归类信息。
type Error struct {
	Kind  ErrorKind
	Cause error
}

func (e *Error) Error() string {
	if e.Cause == nil {
		return e.Kind.String()
	}
	return fmt.Sprintf("%s: %v", e.Kind, e.Cause)
}

func (e *Error) Unwrap() error { return e.Cause }

// Public 返回对外可透出的结论：业务/取消/超时可透出归类与原因，
// 内部错误只透出归类，不得原样透出内部细节。
func (e *Error) Public() string {
	if e.Kind == KindInternal {
		return KindInternal.String()
	}
	return e.Error()
}

// Classify 将任意错误映射为可区分的类型，底层原因被保留。
// 已归类的 *Error 原样返回；context.Canceled / context.DeadlineExceeded
// （包括被 fmt.Errorf %w 包装过的）分别映射为取消与超时；
// 实现 BusinessError 接口的错误归为业务错误；其余归为内部错误。
func Classify(err error) *Error {
	if err == nil {
		return nil
	}
	var ce *Error
	if errors.As(err, &ce) {
		return ce
	}
	switch {
	case errors.Is(err, context.Canceled):
		return &Error{Kind: KindCanceled, Cause: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Kind: KindTimeout, Cause: err}
	}
	var be BusinessError
	if errors.As(err, &be) {
		return &Error{Kind: KindBusiness, Cause: err}
	}
	return &Error{Kind: KindInternal, Cause: err}
}

// BusinessError 由业务方实现以显式声明业务错误。
type BusinessError interface {
	error
	IsBusinessError()
}

// PermanentError 表示接收端明确拒绝该批数据的永久失败：
// 数据本身不被接受，继续重试只会得到同样的结论。
// 导出端返回（或包装）实现该接口的错误时，管线立即停止重试。
type PermanentError interface {
	error
	IsPermanent()
}

// PermanentFailure 把一个错误显式包装为永久失败。
// 已归类为永久失败的错误原样返回，不重复包装。
type PermanentFailure struct {
	Cause error
}

func (e *PermanentFailure) Error() string {
	if e.Cause == nil {
		return "permanent failure"
	}
	return "permanent failure: " + e.Cause.Error()
}

func (e *PermanentFailure) Unwrap() error { return e.Cause }

// IsPermanent 标记该错误为永久失败。
func (e *PermanentFailure) IsPermanent() {}

// MarkPermanent 把任意错误包装为永久失败；nil 原样返回。
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	if IsPermanent(err) {
		return err
	}
	return &PermanentFailure{Cause: err}
}

// IsPermanent 判定错误（含被包装的底层错误）是否为永久失败。
// 取消/超时属于调用方生命周期终止而非接收端拒绝，不作为永久失败；
// 因此判定时先排除 context 错误。
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pe PermanentError
	return errors.As(err, &pe)
}
