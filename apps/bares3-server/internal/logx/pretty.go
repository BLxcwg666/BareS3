package logx

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	ansiReset   = "\033[0m"
	ansiBlack   = "\033[30m"
	ansiRed     = "\033[31m"
	ansiGreen   = "\033[32m"
	ansiYellow  = "\033[33m"
	ansiBlue    = "\033[34m"
	ansiMagenta = "\033[35m"
	ansiCyan    = "\033[36m"
	ansiGray    = "\033[90m"
	ansiBgRed   = "\033[41m"
)

const (
	markerDebug   = "DBG"
	markerInfo    = "INF"
	markerWarn    = "WRN"
	markerError   = "ERR"
	markerFatal   = "FTL"
	markerSuccess = "OK "
	markerReady   = "RDY"
	markerStart   = "RUN"
)

const hintKey = "_hint"

const (
	hintSuccess = "success"
	hintReady   = "ready"
	hintStart   = "start"
)

var lastLogTimeMs atomic.Int64
var prettyBufferPool = buffer.NewPool()

func SuccessField() zap.Field {
	return zap.String(hintKey, hintSuccess)
}

func ReadyField() zap.Field {
	return zap.String(hintKey, hintReady)
}

func StartField() zap.Field {
	return zap.String(hintKey, hintStart)
}

func ShouldColor() bool {
	return os.Getenv("NO_COLOR") == ""
}

func deltaMs() int64 {
	now := time.Now().UnixMilli()
	previous := lastLogTimeMs.Swap(now)
	if previous == 0 {
		return 0
	}
	return now - previous
}

type PrettyEncoder struct {
	color  bool
	fields []field
}

type field struct {
	key string
	val string
}

func NewPrettyEncoder(color bool) zapcore.Encoder {
	return &PrettyEncoder{color: color}
}

func (e *PrettyEncoder) Clone() zapcore.Encoder {
	clone := &PrettyEncoder{
		color:  e.color,
		fields: make([]field, len(e.fields)),
	}
	copy(clone.fields, e.fields)
	return clone
}

func (e *PrettyEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	buf := prettyBufferPool.Get()

	hint := ""
	merged := make([]field, 0, len(e.fields)+len(fields))
	merged = append(merged, e.fields...)

	if len(fields) > 0 {
		collector := &fieldCollector{}
		for _, item := range fields {
			item.AddTo(collector)
		}
		for _, kv := range collector.fields {
			if kv.key == hintKey {
				hint = kv.val
				continue
			}
			merged = append(merged, kv)
		}
	}

	isBadge := entry.Level >= zapcore.ErrorLevel
	if isBadge {
		buf.AppendByte('\n')
	}

	timeText := entry.Time.Format("2006-01-02 15:04:05")
	appendColored(buf, e.color, ansiGray, timeText)
	buf.AppendByte(' ')

	marker, markerColor, badge := resolveMarker(entry.Level, hint)
	if badge {
		label := " " + marker + " "
		if e.color {
			buf.AppendString(ansiBgRed)
			buf.AppendString(ansiBlack)
			buf.AppendString(label)
			buf.AppendString(ansiReset)
		} else {
			buf.AppendString(label)
		}
	} else {
		appendColored(buf, e.color, markerColor, "["+marker+"]")
	}
	buf.AppendByte(' ')

	if entry.LoggerName != "" {
		appendColored(buf, e.color, ansiYellow, "["+entry.LoggerName+"]")
		buf.AppendByte(' ')
	}

	buf.AppendString(entry.Message)
	for _, kv := range merged {
		buf.AppendByte(' ')
		buf.AppendString(kv.key)
		buf.AppendByte('=')
		if needsQuote(kv.val) {
			buf.AppendString(strconv.Quote(kv.val))
		} else {
			buf.AppendString(kv.val)
		}
	}

	delta := deltaMs()
	if delta > 0 {
		appendColored(buf, e.color, ansiYellow, fmt.Sprintf(" +%dms", delta))
	}

	if isBadge {
		buf.AppendByte('\n')
	}
	buf.AppendByte('\n')

	return buf, nil
}

func resolveMarker(level zapcore.Level, hint string) (marker string, color string, badge bool) {
	switch hint {
	case hintSuccess:
		return markerSuccess, ansiGreen, false
	case hintReady:
		return markerReady, ansiGreen, false
	case hintStart:
		return markerStart, ansiMagenta, false
	}

	switch level {
	case zapcore.DebugLevel:
		return markerDebug, ansiGray, false
	case zapcore.InfoLevel:
		return markerInfo, ansiCyan, false
	case zapcore.WarnLevel:
		return markerWarn, ansiYellow, false
	case zapcore.ErrorLevel:
		return markerError, ansiRed, true
	case zapcore.FatalLevel, zapcore.DPanicLevel, zapcore.PanicLevel:
		return markerFatal, ansiRed, true
	default:
		return markerInfo, ansiCyan, false
	}
}

func appendColored(buf *buffer.Buffer, enabled bool, color, text string) {
	if enabled && color != "" {
		buf.AppendString(color)
		buf.AppendString(text)
		buf.AppendString(ansiReset)
		return
	}
	buf.AppendString(text)
}

func needsQuote(value string) bool {
	if value == "" {
		return true
	}
	for i := 0; i < len(value); {
		r, size := utf8.DecodeRuneInString(value[i:])
		if r == ' ' || r == '"' || r == '=' || r == '\n' || r == '\r' || r == '\t' {
			return true
		}
		i += size
	}
	return false
}

func (e *PrettyEncoder) addField(key, value string) {
	e.fields = append(e.fields, field{key: key, val: value})
}

func (e *PrettyEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	collector := &fieldCollector{}
	if err := arr.MarshalLogArray(collector); err != nil {
		return err
	}
	e.addField(key, "["+strings.Join(collector.items, ",")+"]")
	return nil
}

func (e *PrettyEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	collector := &fieldCollector{}
	if err := obj.MarshalLogObject(collector); err != nil {
		return err
	}
	parts := make([]string, 0, len(collector.fields))
	for _, kv := range collector.fields {
		parts = append(parts, kv.key+"="+kv.val)
	}
	e.addField(key, "{"+strings.Join(parts, " ")+"}")
	return nil
}

func (e *PrettyEncoder) AddBinary(key string, value []byte) {
	e.addField(key, fmt.Sprintf("%x", value))
}
func (e *PrettyEncoder) AddByteString(key string, value []byte) { e.addField(key, string(value)) }
func (e *PrettyEncoder) AddBool(key string, value bool)         { e.addField(key, strconv.FormatBool(value)) }
func (e *PrettyEncoder) AddComplex128(key string, value complex128) {
	e.addField(key, fmt.Sprint(value))
}
func (e *PrettyEncoder) AddComplex64(key string, value complex64)    { e.addField(key, fmt.Sprint(value)) }
func (e *PrettyEncoder) AddDuration(key string, value time.Duration) { e.addField(key, value.String()) }
func (e *PrettyEncoder) AddFloat64(key string, value float64) {
	e.addField(key, strconv.FormatFloat(value, 'f', -1, 64))
}
func (e *PrettyEncoder) AddFloat32(key string, value float32) {
	e.addField(key, strconv.FormatFloat(float64(value), 'f', -1, 32))
}
func (e *PrettyEncoder) AddInt(key string, value int) { e.addField(key, strconv.Itoa(value)) }
func (e *PrettyEncoder) AddInt64(key string, value int64) {
	e.addField(key, strconv.FormatInt(value, 10))
}
func (e *PrettyEncoder) AddInt32(key string, value int32) {
	e.addField(key, strconv.FormatInt(int64(value), 10))
}
func (e *PrettyEncoder) AddInt16(key string, value int16) {
	e.addField(key, strconv.FormatInt(int64(value), 10))
}
func (e *PrettyEncoder) AddInt8(key string, value int8) {
	e.addField(key, strconv.FormatInt(int64(value), 10))
}
func (e *PrettyEncoder) AddString(key string, value string) { e.addField(key, value) }
func (e *PrettyEncoder) AddTime(key string, value time.Time) {
	e.addField(key, value.Format(time.RFC3339))
}
func (e *PrettyEncoder) AddUint(key string, value uint) {
	e.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (e *PrettyEncoder) AddUint64(key string, value uint64) {
	e.addField(key, strconv.FormatUint(value, 10))
}
func (e *PrettyEncoder) AddUint32(key string, value uint32) {
	e.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (e *PrettyEncoder) AddUint16(key string, value uint16) {
	e.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (e *PrettyEncoder) AddUint8(key string, value uint8) {
	e.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (e *PrettyEncoder) AddUintptr(key string, value uintptr) {
	e.addField(key, fmt.Sprintf("0x%x", value))
}
func (e *PrettyEncoder) AddReflected(key string, value interface{}) error {
	e.addField(key, fmt.Sprint(value))
	return nil
}
func (e *PrettyEncoder) OpenNamespace(key string) {
	for index := range e.fields {
		e.fields[index].key = key + "." + e.fields[index].key
	}
}

type fieldCollector struct {
	fields []field
	items  []string
}

func (c *fieldCollector) addField(key, value string) {
	c.fields = append(c.fields, field{key: key, val: value})
}

func (c *fieldCollector) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	c.addField(key, "<array>")
	return nil
}
func (c *fieldCollector) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	c.addField(key, "<object>")
	return nil
}
func (c *fieldCollector) AddBinary(key string, value []byte) {
	c.addField(key, fmt.Sprintf("%x", value))
}
func (c *fieldCollector) AddByteString(key string, value []byte) { c.addField(key, string(value)) }
func (c *fieldCollector) AddBool(key string, value bool)         { c.addField(key, strconv.FormatBool(value)) }
func (c *fieldCollector) AddComplex128(key string, value complex128) {
	c.addField(key, fmt.Sprint(value))
}
func (c *fieldCollector) AddComplex64(key string, value complex64) {
	c.addField(key, fmt.Sprint(value))
}
func (c *fieldCollector) AddDuration(key string, value time.Duration) {
	c.addField(key, value.String())
}
func (c *fieldCollector) AddFloat64(key string, value float64) {
	c.addField(key, strconv.FormatFloat(value, 'f', -1, 64))
}
func (c *fieldCollector) AddFloat32(key string, value float32) {
	c.addField(key, strconv.FormatFloat(float64(value), 'f', -1, 32))
}
func (c *fieldCollector) AddInt(key string, value int) { c.addField(key, strconv.Itoa(value)) }
func (c *fieldCollector) AddInt64(key string, value int64) {
	c.addField(key, strconv.FormatInt(value, 10))
}
func (c *fieldCollector) AddInt32(key string, value int32) {
	c.addField(key, strconv.FormatInt(int64(value), 10))
}
func (c *fieldCollector) AddInt16(key string, value int16) {
	c.addField(key, strconv.FormatInt(int64(value), 10))
}
func (c *fieldCollector) AddInt8(key string, value int8) {
	c.addField(key, strconv.FormatInt(int64(value), 10))
}
func (c *fieldCollector) AddString(key string, value string) { c.addField(key, value) }
func (c *fieldCollector) AddTime(key string, value time.Time) {
	c.addField(key, value.Format(time.RFC3339))
}
func (c *fieldCollector) AddUint(key string, value uint) {
	c.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AddUint64(key string, value uint64) {
	c.addField(key, strconv.FormatUint(value, 10))
}
func (c *fieldCollector) AddUint32(key string, value uint32) {
	c.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AddUint16(key string, value uint16) {
	c.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AddUint8(key string, value uint8) {
	c.addField(key, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AddUintptr(key string, value uintptr) {
	c.addField(key, fmt.Sprintf("0x%x", value))
}
func (c *fieldCollector) AddReflected(key string, value interface{}) error {
	c.addField(key, fmt.Sprint(value))
	return nil
}
func (c *fieldCollector) OpenNamespace(_ string) {}

func (c *fieldCollector) AppendBool(value bool)         { c.items = append(c.items, strconv.FormatBool(value)) }
func (c *fieldCollector) AppendByteString(value []byte) { c.items = append(c.items, string(value)) }
func (c *fieldCollector) AppendComplex128(value complex128) {
	c.items = append(c.items, fmt.Sprint(value))
}
func (c *fieldCollector) AppendComplex64(value complex64) {
	c.items = append(c.items, fmt.Sprint(value))
}
func (c *fieldCollector) AppendDuration(value time.Duration) {
	c.items = append(c.items, value.String())
}
func (c *fieldCollector) AppendFloat64(value float64) {
	c.items = append(c.items, strconv.FormatFloat(value, 'f', -1, 64))
}
func (c *fieldCollector) AppendFloat32(value float32) {
	c.items = append(c.items, strconv.FormatFloat(float64(value), 'f', -1, 32))
}
func (c *fieldCollector) AppendInt(value int) { c.items = append(c.items, strconv.Itoa(value)) }
func (c *fieldCollector) AppendInt64(value int64) {
	c.items = append(c.items, strconv.FormatInt(value, 10))
}
func (c *fieldCollector) AppendInt32(value int32) {
	c.items = append(c.items, strconv.FormatInt(int64(value), 10))
}
func (c *fieldCollector) AppendInt16(value int16) {
	c.items = append(c.items, strconv.FormatInt(int64(value), 10))
}
func (c *fieldCollector) AppendInt8(value int8) {
	c.items = append(c.items, strconv.FormatInt(int64(value), 10))
}
func (c *fieldCollector) AppendString(value string) { c.items = append(c.items, value) }
func (c *fieldCollector) AppendTime(value time.Time) {
	c.items = append(c.items, value.Format(time.RFC3339))
}
func (c *fieldCollector) AppendUint(value uint) {
	c.items = append(c.items, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AppendUint64(value uint64) {
	c.items = append(c.items, strconv.FormatUint(value, 10))
}
func (c *fieldCollector) AppendUint32(value uint32) {
	c.items = append(c.items, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AppendUint16(value uint16) {
	c.items = append(c.items, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AppendUint8(value uint8) {
	c.items = append(c.items, strconv.FormatUint(uint64(value), 10))
}
func (c *fieldCollector) AppendUintptr(value uintptr) {
	c.items = append(c.items, fmt.Sprintf("0x%x", value))
}
func (c *fieldCollector) AppendReflected(value interface{}) error {
	c.items = append(c.items, fmt.Sprint(value))
	return nil
}
func (c *fieldCollector) AppendArray(value zapcore.ArrayMarshaler) error {
	return value.MarshalLogArray(c)
}
func (c *fieldCollector) AppendObject(value zapcore.ObjectMarshaler) error {
	c.items = append(c.items, "<object>")
	return nil
}

var processStartOnce sync.Once
var processStartTime time.Time

func MarkProcessStart() {
	processStartOnce.Do(func() {
		processStartTime = time.Now()
	})
}

func init() {
	MarkProcessStart()
}
