// Package hclog contains an adaptor for the hashicorp/raft hclog logger interface.
package hclog

import (
	"context"
	"fmt"
	"io"
	stdlog "log"

	"github.com/hashicorp/go-hclog"

	"go.atoms.co/lib/log"
)

type logger struct {
	name   string
	ctx    context.Context
	cutoff log.Severity
}

// New creates a new hclog from the global standard logger. Used in hashicorp libraries to standardize logs
func New(ctx context.Context, name string, cutoff log.Severity) hclog.Logger {
	return &logger{name: name, ctx: ctx, cutoff: cutoff}
}

func (l *logger) Log(level hclog.Level, msg string, args ...interface{}) {
	l.log(level, msg, args)
}

func (l *logger) Trace(msg string, args ...interface{}) {
	l.log(hclog.Trace, msg, args...)
}

func (l *logger) Debug(msg string, args ...interface{}) {
	l.log(hclog.Debug, msg, args...)
}

func (l *logger) Info(msg string, args ...interface{}) {
	l.log(hclog.Info, msg, args...)
}

func (l *logger) Warn(msg string, args ...interface{}) {
	l.log(hclog.Warn, msg, args...)
}

func (l *logger) Error(msg string, args ...interface{}) {
	l.log(hclog.Error, msg, args...)
}

func (l *logger) log(level hclog.Level, msg string, args ...interface{}) {
	log.Output(toFields(l.ctx, args), toSeverity(level), 2, fmt.Sprintf("%s %v", l.name, msg))
}

func (l *logger) IsTrace() bool {
	return l.cutoff <= log.SevDebug
}

func (l *logger) IsDebug() bool {
	return l.cutoff <= log.SevDebug
}

func (l *logger) IsInfo() bool {
	return l.cutoff <= log.SevInfo
}

func (l *logger) IsWarn() bool {
	return l.cutoff <= log.SevWarn
}

func (l *logger) IsError() bool {
	return l.cutoff <= log.SevError
}

func (l *logger) ImpliedArgs() []interface{} {
	return fromFields(l.ctx)
}

func (l *logger) With(args ...interface{}) hclog.Logger {
	return New(toFields(l.ctx, args), l.name, l.cutoff)
}

func (l *logger) Name() string {
	return l.name
}

func (l *logger) Named(name string) hclog.Logger {
	if l.name != "" {
		name = fmt.Sprintf("%s %s", l.name, name)
	}
	return New(l.ctx, name, l.cutoff)
}

func (l *logger) ResetNamed(name string) hclog.Logger {
	return New(l.ctx, name, l.cutoff)
}

func (l *logger) SetLevel(level hclog.Level) {
	// no-op, ok per spec
}

func (l *logger) GetLevel() hclog.Level {
	return fromSeverity(l.cutoff)
}

func (l *logger) StandardLogger(opts *hclog.StandardLoggerOptions) *stdlog.Logger {
	// TODO(jhhurwitz): 07/31/2023 Do we need this?
	return nil
}

func (l *logger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	// TODO(jhhurwitz): 07/31/2023 Do we need this?
	return nil
}

// Expect alternating key(string),value(any) pairs https://github.com/hashicorp/go-hclog/blob/main/logger.go#L149
func toFields(ctx context.Context, args []interface{}) context.Context {
	var fields []log.Field
	for i := 0; i < len(args)/2; i++ {
		idx := i * 2

		key := args[idx]
		value := args[idx+1]
		fields = append(fields, log.Field{
			Key:    fmt.Sprintf("%v", key),
			String: fmt.Sprintf("%v", value),
			Type:   log.StringType,
		})
	}
	return log.NewContext(ctx, fields...)
}

func fromFields(ctx context.Context) []interface{} {
	fields := log.FromContext(ctx)
	args := make([]interface{}, len(fields))
	for _, field := range fields {
		if field.Type == log.SkipType || field.Type == log.UnknownType {
			continue
		}
		args = append(args, field.Key)
		switch field.Type {
		case log.BoolType, log.Float32Type, log.Float64Type, log.Int8Type, log.Int16Type, log.Int32Type, log.Int64Type, log.Uint8Type, log.Uint16Type, log.Uint32Type, log.Uint64Type, log.UintptrType, log.DurationType:
			args = append(args, field.Integer)
		case log.StringType:
			args = append(args, field.String)
		case log.BinaryType, log.ByteStringType, log.Complex64Type, log.Complex128Type, log.StringerType, log.TimeFullType, log.ErrorType:
			args = append(args, field.Interface)
		case log.TimeType:
			args = append(args, fmt.Sprintf("%v:%v", field.Integer, field.Interface))
		default:
			args = append(args, "unknown")
		}
	}
	return args
}

func toSeverity(level hclog.Level) log.Severity {
	switch level {
	case hclog.Trace, hclog.Debug:
		return log.SevDebug
	case hclog.Info:
		return log.SevInfo
	case hclog.Warn:
		return log.SevWarn
	case hclog.Error:
		return log.SevError
	default:
		return log.SevInfo
	}
}

func fromSeverity(sev log.Severity) hclog.Level {
	switch sev {
	case log.SevDebug:
		return hclog.Debug
	case log.SevInfo:
		return hclog.Info
	case log.SevWarn:
		return hclog.Warn
	case log.SevError:
		return hclog.Error
	default:
		return hclog.Info
	}
}
