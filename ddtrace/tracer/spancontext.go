// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/internal/tracerstats"
	traceinternal "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer/internal"
	sharedinternal "github.com/DataDog/dd-trace-go/v2/internal"
	internalconfig "github.com/DataDog/dd-trace-go/v2/internal/config"
	"github.com/DataDog/dd-trace-go/v2/internal/locking"
	"github.com/DataDog/dd-trace-go/v2/internal/locking/assert"
	"github.com/DataDog/dd-trace-go/v2/internal/log"
	"github.com/DataDog/dd-trace-go/v2/internal/samplernames"
)

const TraceIDZero string = "00000000000000000000000000000000"

// traceID128BitEnabled caches DD_TRACE_128_BIT_TRACEID_GENERATION_ENABLED
// at init time so that newSpanContext avoids calling BoolEnv on every span.
// It is re-set by newConfig on every tracer start so that restarts pick up
// any changed value. The init here ensures it is correct even when using
// mocktracer (which does not call newConfig).
var traceID128BitEnabled atomic.Bool

func init() {
	traceID128BitEnabled.Store(sharedinternal.BoolEnv("DD_TRACE_128_BIT_TRACEID_GENERATION_ENABLED", true))
}

var _ ddtrace.SpanContext = (*SpanContext)(nil)

// traceID in big endian, i.e. <upper><lower>
type traceID struct {
	value      [16]byte
	hexEncoded string
}

func (t *traceID) HexEncoded() string {
	if t.hexEncoded == "" {
		t.computeAndCacheHex()
	}
	return t.hexEncoded
}

func (t *traceID) Lower() uint64 {
	return binary.BigEndian.Uint64(t.value[8:])
}

func (t *traceID) Upper() uint64 {
	return binary.BigEndian.Uint64(t.value[:8])
}

func (t *traceID) SetLower(i uint64) {
	binary.BigEndian.PutUint64(t.value[8:], i)
	t.hexEncoded = ""
}

func (t *traceID) SetUpper(i uint64) {
	binary.BigEndian.PutUint64(t.value[:8], i)
	t.hexEncoded = ""
}

func (t *traceID) set(v [16]byte) {
	t.value = v
	t.hexEncoded = ""
}

func (t *traceID) SetUpperFromHex(s string) error {
	u, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return fmt.Errorf("malformed %q: %s", s, err)
	}
	t.SetUpper(u)
	return nil
}

func (t *traceID) Empty() bool {
	return t.value == [16]byte{}
}

func (t *traceID) HasUpper() bool {
	for ix := range t.value[:8] {
		if t.value[ix] != 0 {
			return true
		}
	}
	return false
}

func (t *traceID) UpperHex() string { return t.HexEncoded()[:16] }

func (t *traceID) computeAndCacheHex() {
	t.hexEncoded = hex.EncodeToString(t.value[:])
}

// SpanContext represents a span state that can propagate to descendant spans
// and across process boundaries. It contains all the information needed to
// spawn a direct descendant of the span that it belongs to. It can be used
// to create distributed tracing by propagating it using the provided interfaces.
type SpanContext struct {
	updated bool // updated is tracking changes for priority / origin / x-datadog-tags

	// the below group should propagate only locally

	trace  *trace       // reference to the trace that this span belongs too
	span   *Span        // reference to the span that hosts this context
	errors atomic.Int32 // number of spans with errors in this trace

	// The 16-character hex string of the last seen Datadog Span ID
	// this value will be added as the _dd.parent_id tag to spans
	// created from this spanContext.
	// This value is extracted from the `p` sub-key within the tracestate.
	// The backend will use the _dd.parent_id tag to reparent spans in
	// distributed traces if they were missing their parent span.
	// Missing parent span could occur when a W3C-compliant tracer
	// propagated this context, but didn't send any spans to Datadog.
	reparentID string
	isRemote   bool

	// the below group should propagate cross-process

	traceID traceID
	spanID  uint64

	// guards below fields
	mu locking.RWMutex
	// +checklocks:mu
	baggage map[string]string
	// atomic int for quick checking presence of baggage. 0 indicates no baggage, otherwise baggage exists.
	hasBaggage uint32 // +checkatomic
	// e.g. "synthetics"
	// +checklocks:mu
	origin string

	// links to related spans in separate|external|disconnected traces
	// +checklocks:mu
	spanLinks []SpanLink
	// when true, indicates this context only propagates baggage items and should not be used for distributed tracing fields
	// +checklocks:mu
	baggageOnly bool
}

// Private interface for span contexts that can propagate sampling decisions.
type spanContextWithSamplingDecision interface {
	SamplingDecision() uint32
	Priority() *float64
}

// Private interface for converting v1 span contexts to v2 ones.
type spanContextV1Adapter interface {
	spanContextWithSamplingDecision
	Origin() string
	PropagatingTags() map[string]string
	Tags() map[string]string
}

// FromGenericCtx converts a ddtrace.SpanContext to a *SpanContext, which can be used
// to start child spans.
func FromGenericCtx(c ddtrace.SpanContext) *SpanContext {
	var sc SpanContext
	sc.traceID.set(c.TraceIDBytes())
	sc.spanID = c.SpanID()
	sc.baggage = make(map[string]string) // +checklocksignore - Initialization time, not shared yet.
	c.ForeachBaggageItem(func(k, v string) bool {
		sc.hasBaggage = 1 // +checklocksignore - Initialization time, not shared yet.
		sc.baggage[k] = v // +checklocksignore - Initialization time, not shared yet.
		return true
	})

	ctxSpl, ok := c.(spanContextWithSamplingDecision)
	if !ok {
		return &sc
	}

	// Translate the external sampling decision into DD trace semantics.
	//
	// Two separate mechanisms are at play:
	//
	//   - samplingDecision (decisionNone/Keep/Drop): controls whether the trace
	//     payload is sent to the agent (willSend) and whether keep()/drop() can
	//     modify it. Both keep() and drop() use atomic Compare-And-Swap (CAS)
	//     from decisionNone only — they atomically check that the current value
	//     is decisionNone before writing, so whichever goroutine calls first
	//     wins and subsequent calls are no-ops. This lets error spans "rescue"
	//     a trace: if keep() runs before drop(), the trace is sent.
	//
	//   - sampling priority (P0/P1) + lock: the numeric priority sent to the
	//     agent. Locking prevents the DD sampler from overriding it via
	//     resampling.
	//
	// For keep decisions (OTel sampled=true): propagate decisionKeep directly
	// so the trace is guaranteed to be sent.
	//
	// For drop decisions (OTel sampled=false): set priority to P0 and lock it
	// (so the DD sampler won't override the OTel decision), but leave
	// samplingDecision as decisionNone. This preserves the CAS semantics: if
	// an error span finishes, keep() can still CAS(decisionNone -> decisionKeep)
	// and rescue the trace. If no span rescues it, drop() will
	// CAS(decisionNone -> decisionDrop) and the trace is dropped normally.
	// This matches native DD tracer behavior where P0 traces are not
	// hard-dropped client-side.
	if sDecision := samplingDecision(ctxSpl.SamplingDecision()); sDecision != decisionNone {
		sc.trace = newTrace()
		if sDecision == decisionKeep {
			sc.trace.samplingDecision = sDecision // +checklocksignore - Initialization time, not shared yet.
		}
		// For decisionDrop we intentionally leave samplingDecision as
		// decisionNone (the newTrace default). The priority below still
		// records the OTel intent (P0), and locking prevents resampling.

		if p := ctxSpl.Priority(); p != nil {
			sc.setSamplingPriority(int(*p), samplernames.Unknown)
			sc.trace.setLocked(true)
		}
	}

	ctx, ok := c.(spanContextV1Adapter)
	if !ok {
		return &sc
	}

	sc.origin = ctx.Origin() // +checklocksignore - Initialization time, not shared yet.
	if sc.trace == nil {
		sc.trace = newTrace()
	}
	sc.trace.tags = ctx.Tags()                                    // +checklocksignore - Initialization time, not shared yet.
	sc.trace.propagatingTags = ctx.PropagatingTags()              // +checklocksignore - Initialization time, not shared yet.
	if dm, ok := sc.trace.propagatingTags[keyDecisionMaker]; ok { // +checklocksignore - Initialization time, not shared yet.
		sc.trace.dm = parseDecisionMaker(dm) // +checklocksignore - Initialization time, not shared yet.
	}
	return &sc
}

// newSpanContext creates a new SpanContext to serve as context for the given
// span. If the provided parent is not nil, the context will inherit the trace,
// baggage and other values from it. This method also pushes the span into the
// new context's trace and as a result, it should not be called multiple times
// for the same span.
// +checklocksignore — Initialization time, context not yet shared.
func newSpanContext(span *Span, parent *SpanContext) *SpanContext {
	context := &SpanContext{
		spanID: span.spanID,
		span:   span,
	}

	context.traceID.SetLower(span.traceID)
	if parent != nil {
		if !parent.baggageOnly { // +checklocksignore - Read-only after init.
			context.traceID.SetUpper(parent.traceID.Upper())
			context.trace = parent.trace
			context.origin = parent.origin // +checklocksignore - Initialization time, not shared yet. Parent origin is read-only after init.
			context.errors.Store(parent.errors.Load())
		}
		parent.ForeachBaggageItem(func(k, v string) bool {
			context.setBaggageItem(k, v)
			return true
		})
	} else if traceID128BitEnabled.Load() {
		// add 128 bit trace id, if enabled, formatted as big-endian:
		// <32-bit unix seconds> <32 bits of zero> <64 random bits>
		id128 := time.Duration(span.start) / time.Second
		// casting from int64 -> uint32 should be safe since the start time won't be
		// negative, and the seconds should fit within 32-bits for the foreseeable future.
		// (We only want 32 bits of time, then the rest is zero)
		tUp := uint64(uint32(id128)) << 32 // We need the time at the upper 32 bits of the uint
		context.traceID.SetUpper(tUp)
	}
	if context.trace == nil {
		context.trace = newTrace()
	}
	if context.trace.root == nil {
		// first span in the trace can safely be assumed to be the root
		context.trace.root = span
	}
	// put span in context's trace
	context.trace.push(span)
	// setting context.updated to false here is necessary to distinguish
	// between initializing properties of the span (priority)
	// and updating them after extracting context through propagators
	context.updated = false
	return context
}

// SpanID implements ddtrace.SpanContext.
func (c *SpanContext) SpanID() uint64 {
	if c == nil {
		return 0
	}
	return c.spanID
}

// TraceID implements ddtrace.SpanContext.
func (c *SpanContext) TraceID() string {
	if c == nil || c.traceID.Empty() {
		return TraceIDZero
	}
	return c.traceID.HexEncoded()
}

// TraceIDBytes implements ddtrace.SpanContext.
func (c *SpanContext) TraceIDBytes() [16]byte {
	if c == nil {
		return [16]byte{}
	}
	return c.traceID.value
}

// TraceIDLower implements ddtrace.SpanContext.
func (c *SpanContext) TraceIDLower() uint64 {
	if c == nil {
		return 0
	}
	return c.traceID.Lower()
}

// TraceIDUpper implements ddtrace.SpanContext.
func (c *SpanContext) TraceIDUpper() uint64 {
	if c == nil {
		return 0
	}
	return c.traceID.Upper()
}

// SpanLinks implements ddtrace.SpanContext
func (c *SpanContext) SpanLinks() []SpanLink {
	cp := make([]SpanLink, len(c.spanLinks)) // +checklocksignore - Read-only after init.
	copy(cp, c.spanLinks)                    // +checklocksignore - Read-only after init.
	return cp
}

// foreachBaggageItemLocked iterates over baggage items.
// c.mu must be held for reading.
// +checklocksread:c.mu
func (c *SpanContext) foreachBaggageItemLocked(handler func(k, v string) bool) {
	assert.RWMutexRLocked(&c.mu)
	for k, v := range c.baggage {
		if !handler(k, v) {
			break
		}
	}
}

// ForeachBaggageItem implements ddtrace.SpanContext.
func (c *SpanContext) ForeachBaggageItem(handler func(k, v string) bool) {
	if c == nil {
		return
	}
	if atomic.LoadUint32(&c.hasBaggage) == 0 {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.foreachBaggageItemLocked(handler)
}

// sets the sampling priority and decision maker (based on `sampler`).
func (c *SpanContext) setSamplingPriority(p int, sampler samplernames.SamplerName) {
	if c.trace == nil {
		c.trace = newTrace()
	}
	if c.trace.setSamplingPriority(p, sampler) {
		// the trace's sampling priority or sampler was updated: mark this as updated
		c.updated = true
	}
}

// forceSetSamplingPriority sets (and forces if the trace is locked) the sampling priority and decision maker (based on `sampler`).
func (c *SpanContext) forceSetSamplingPriority(p int, sampler samplernames.SamplerName) {
	if c.trace == nil {
		c.trace = newTrace()
	}
	if c.trace.forceSetSamplingPriority(p, sampler) {
		// the trace's sampling priority or sampler was updated: mark this as updated
		c.updated = true
	}
}

func (c *SpanContext) SamplingPriority() (p int, ok bool) {
	if c == nil || c.trace == nil {
		return 0, false
	}
	return c.trace.samplingPriority()
}

// setBaggageItemLocked sets a baggage item.
// c.mu must be held for writing.
// +checklocks:c.mu
func (c *SpanContext) setBaggageItemLocked(key, val string) {
	assert.RWMutexLocked(&c.mu)
	if c.baggage == nil {
		atomic.StoreUint32(&c.hasBaggage, 1)
		c.baggage = make(map[string]string, 1)
	}
	c.baggage[key] = val
}

func (c *SpanContext) setBaggageItem(key, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setBaggageItemLocked(key, val)
}

// baggageItemLocked retrieves a baggage item.
// c.mu must be held for reading.
// +checklocksread:c.mu
func (c *SpanContext) baggageItemLocked(key string) string {
	assert.RWMutexRLocked(&c.mu)
	return c.baggage[key]
}

// baggageCountLocked returns the number of baggage items.
// c.mu must be held for reading.
// +checklocksread:c.mu
func (c *SpanContext) baggageCountLocked() int {
	assert.RWMutexRLocked(&c.mu)
	return len(c.baggage)
}

func (c *SpanContext) baggageItem(key string) string {
	if atomic.LoadUint32(&c.hasBaggage) == 0 {
		return ""
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.baggageItemLocked(key)
}

// finish marks this span as finished in the trace.
// The span must be locked by the caller.
func (c *SpanContext) finish() {
	c.trace.finishedOneLocked(c.span)
}

// safeDebugString returns a safe string representation of the SpanContext for debug logging.
// It excludes potentially sensitive data like baggage contents while preserving useful debugging information.
func (c *SpanContext) safeDebugString() string {
	if c == nil {
		return "<nil>"
	}

	hasBaggage := atomic.LoadUint32(&c.hasBaggage) != 0
	var baggageCount int
	if hasBaggage {
		c.mu.RLock()
		baggageCount = c.baggageCountLocked()
		c.mu.RUnlock()
	}

	origin := c.origin           // +checklocksignore - Read-only after init.
	baggageOnly := c.baggageOnly // +checklocksignore - Read-only after init.
	return fmt.Sprintf("SpanContext{traceID=%s, spanID=%d, hasBaggage=%t, baggageCount=%d, origin=%q, updated=%t, isRemote=%t, baggageOnly=%t}",
		c.TraceID(), c.SpanID(), hasBaggage, baggageCount, origin, c.updated, c.isRemote, baggageOnly)
}

// samplingDecision is the decision to send a trace to the agent or not.
type samplingDecision uint32

const (
	// decisionNone is the default state of a trace.
	// If no decision is made about the trace, the trace won't be sent to the agent.
	decisionNone samplingDecision = iota
	// decisionDrop prevents the trace from being sent to the agent.
	decisionDrop
	// decisionKeep ensures the trace will be sent to the agent.
	decisionKeep
)

// trace contains shared context information about a trace, such as sampling
// priority, the root reference and a buffer of the spans which are part of the
// trace, if these exist.
type trace struct {
	// started counts spans pushed into this trace via push(); finished counts
	// spans that have called finishedOneLocked(). When finished == started the
	// trace is complete. Both are updated atomically; no lock required.
	started  atomic.Int64 // +checkatomic
	finished atomic.Int64 // +checkatomic

	// dropped is set when started exceeds traceMaxSize. Subsequent push() and
	// finishedOneLocked() calls are no-ops.
	dropped atomic.Bool // +checkatomic

	// rescued is set when at least one span from this trace was rescued by
	// single-span sampling even though the trace decision was drop. Used to
	// correctly count DroppedP0Traces only when truly nothing was sent.
	rescued atomic.Bool // +checkatomic

	// mu guards metadata: tags, propagatingTags, priority, locked, dm.
	// It is no longer held during the span-creation hot path (push) or
	// the common span-finish path (finishedOneLocked).
	mu locking.RWMutex
	// trace level tags
	// +checklocks:mu
	tags map[string]string
	// trace level tags that will be propagated across service boundaries
	// +checklocks:mu
	propagatingTags map[string]string
	// sampling priority — accessed atomically to allow lock-free reads
	// from the span creation hot path (SamplingPriority).
	// Writes still happen under mu (because they also touch propagatingTags).
	// +checkatomic
	priority atomic.Pointer[float64]
	// specifies if the sampling priority can be altered
	// +checklocks:mu
	locked bool
	// dm is the numeric form of _dd.p.dm for v1 protocol encoding.
	// It is the absolute value of the parsed integer (e.g., "-4" → 4).
	// +checklocks:mu
	dm uint32
	// samplingDecision indicates whether to send the trace to the agent.
	samplingDecision samplingDecision // +checkatomic

	// root specifies the root of the trace, if known; it is nil when a span
	// context is extracted from a carrier, at which point there are no spans in
	// the trace yet.
	// Write-once during initialization in newSpanContext, read-only afterward.
	root *Span
}

var (
	// traceStartSize is the initial size of our trace buffer,
	// by default we allocate for a handful of spans within the trace,
	// reasonable as span is actually way bigger, and avoids re-allocating
	// over and over. Could be fine-tuned at runtime.
	traceStartSize = 10
	traceMaxSize   = internalconfig.TraceMaxSize
)

// samplingPriorityCache holds pre-allocated pointers for the four standard
// sampling priority values defined in ddtrace/ext/priority.go:
//
//	index 0 → -1 (ext.PriorityUserReject)
//	index 1 →  0 (ext.PriorityAutoReject)
//	index 2 →  1 (ext.PriorityAutoKeep)
//	index 3 →  2 (ext.PriorityUserKeep)
//
// Caching these avoids a heap allocation on every call to
// setSamplingPriorityLockedWithForce, which is on the hot path.
var samplingPriorityCache = func() [4]*float64 {
	var c [4]*float64
	for i := range c {
		v := float64(i - 1) // -1, 0, 1, 2
		c[i] = &v
	}
	return c
}()

// samplingPriorityPtr returns a *float64 for p without allocating for the
// standard priority values (ext.PriorityUserReject through ext.PriorityUserKeep);
// for any other value it allocates a new one.
func samplingPriorityPtr(p int) *float64 {
	if p >= -1 && p <= 2 {
		return samplingPriorityCache[p+1]
	}
	v := float64(p)
	return &v
}

// newTrace creates a new trace.
func newTrace() *trace {
	return &trace{}
}

// samplingPriority returns the sampling priority of the trace, if set.
// This is safe to call without holding t.mu because priority is an atomic pointer.
func (t *trace) samplingPriority() (p int, ok bool) {
	priority := t.priority.Load() // +checklocksignore
	if priority == nil {
		return 0, false
	}
	return int(*priority), true
}

// setSamplingPriority sets the sampling priority and the decision maker
// and returns true if it was modified.
func (t *trace) setSamplingPriority(p int, sampler samplernames.SamplerName) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.setSamplingPriorityLocked(p, sampler)
}

// forceSetSamplingPriority forces the sampling priority and the decision maker
// and returns true if it was modified.
func (t *trace) forceSetSamplingPriority(p int, sampler samplernames.SamplerName) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.setSamplingPriorityLockedWithForce(p, sampler, true)
}

func (t *trace) keep() {
	atomic.CompareAndSwapUint32((*uint32)(&t.samplingDecision), uint32(decisionNone), uint32(decisionKeep))
}

func (t *trace) drop() {
	atomic.CompareAndSwapUint32((*uint32)(&t.samplingDecision), uint32(decisionNone), uint32(decisionDrop))
}

func (t *trace) setTag(key, value string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setTagLocked(key, value)
}

// +checklocks:t.mu
func (t *trace) setTagLocked(key, value string) {
	assert.RWMutexLocked(&t.mu)
	if t.tags == nil {
		t.tags = make(map[string]string, 1)
	}
	t.tags[key] = value
}

// setSamplingPriority sets the sampling priority and the decision maker
// and returns true if it was modified.
//
// The force parameter is used to bypass the locked sampling decision check
// when setting the sampling priority. This is used to apply a manual keep or drop decision.
// +checklocks:t.mu
func (t *trace) setSamplingPriorityLockedWithForce(p int, sampler samplernames.SamplerName, force bool) bool {
	assert.RWMutexLocked(&t.mu)
	if t.locked && !force {
		return false
	}

	old := t.priority.Load() // +checklocksignore
	updatedPriority := old == nil || *old != float64(p)

	t.priority.Store(samplingPriorityPtr(p)) // +checklocksignore
	curDM, existed := t.propagatingTags[keyDecisionMaker]
	if p > 0 && sampler != samplernames.Unknown {
		// We have a positive priority and the sampling mechanism isn't set.
		// Send nothing when sampler is `Unknown` for RFC compliance.
		// If a global sampling rate is set, it was always applied first. And this call can be
		// triggered again by applying a rule sampler. The sampling priority will be the same, but
		// the decision maker will be different. So we compare the decision makers as well.
		// Note that once global rate sampling is deprecated, we no longer need to compare
		// the DMs. Sampling priority is sufficient to distinguish a change in DM.
		dm := sampler.DecisionMaker()
		updatedDM := !existed || dm != curDM
		if updatedDM {
			t.setPropagatingTagLocked(keyDecisionMaker, dm)
			return true
		}
	}
	if p <= 0 && existed {
		t.unsetPropagatingTagLocked(keyDecisionMaker)
	}

	return updatedPriority
}

// +checklocks:t.mu
func (t *trace) setSamplingPriorityLocked(p int, sampler samplernames.SamplerName) bool {
	assert.RWMutexLocked(&t.mu)
	return t.setSamplingPriorityLockedWithForce(p, sampler, false)
}

func (t *trace) isLocked() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.locked
}

func (t *trace) setLocked(locked bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.locked = locked
}

// push registers a new span with the trace.
// +checklocksignore — sp.metrics is read before the span is shared.
func (t *trace) push(sp *Span) {
	if t.dropped.Load() {
		return
	}
	n := t.started.Add(1)
	if n > int64(traceMaxSize) {
		// Only log and signal on the first overflow.
		if n == int64(traceMaxSize)+1 {
			log.Error("trace buffer full (%d spans), dropping trace", traceMaxSize)
			tracerstats.Signal(tracerstats.TracesDropped, 1)
		}
		t.dropped.Store(true)
		return
	}
	tracerstats.Signal(tracerstats.SpanStarted, 1)
	if v, ok := sp.metrics[keySamplingPriority]; ok {
		// Rare: explicit priority on a span — update trace metadata under mu.
		t.setSamplingPriority(int(v), samplernames.Unknown)
	}
}

// setTraceTagsLocked sets all "trace level" tags on the provided span.
// Caller must hold t.mu for reading and s.mu for writing.
// +checklocksread:t.mu
// +checklocks:s.mu
func (t *trace) setTraceTagsLocked(s *Span) {
	assert.RWMutexRLocked(&t.mu)
	assert.RWMutexLocked(&s.mu)
	for k, v := range t.tags {
		s.setMetaLocked(k, v)
	}
	for k, v := range t.propagatingTags {
		s.setMetaLocked(k, v)
	}
	updateTracerGitMetadataTags(s)
	if s.context != nil && s.context.traceID.HasUpper() {
		s.setMetaLocked(keyTraceID128, s.context.traceID.UpperHex())
	}
}

// updateTracerGitMetadataTags updates the tracer git metadata tags on the given span.
// +checklocks:s.mu
func updateTracerGitMetadataTags(s *Span) {
	assert.RWMutexLocked(&s.mu)
	gitMetadataTags := sharedinternal.GetGitMetadataTags()
	for ix := range sharedinternal.TracerGitMetadataKeys {
		pair := sharedinternal.TracerGitMetadataKeys[ix]
		src, dst := pair[0], pair[1]
		if v := gitMetadataTags[src]; v != "" {
			s.setMetaLocked(dst, v)
		}
	}
}

// finishedOneLocked is called when span s has finished. The caller holds s.mu.
//
// In the send-on-finish model each span is submitted to the writer immediately
// after it finishes; the trace.mu write lock is no longer on the hot path.
// The only mu acquisitions here are:
//   - RLock to read t.tags for setTraceTagsLocked (brief, allows concurrent readers)
//   - Lock to set t.locked when the root span finishes (once per trace)
//
// Lock ordering: span.mu → trace.mu (same as before).
// +checklocksignore — Caller holds s.mu; cross-struct ordering verified by convention.
func (t *trace) finishedOneLocked(s *Span) {
	assert.RWMutexLocked(&s.mu)

	if t.dropped.Load() {
		return
	}
	if s.finished {
		return
	}
	s.finished = true

	tr := getGlobalTracer()
	if tr == nil {
		t.finished.Add(1)
		return
	}
	tc := tr.TracerConf()

	// Per-span metadata — s.mu already held by caller.
	setPeerService(s, tc)
	if s.service != "" && !strings.EqualFold(s.service, tc.ServiceTag) {
		s.setMetaLocked(keyBaseService, tc.ServiceTag)
	}

	// Apply trace-level tags to the root span so the agent can find them.
	// The root span is the canonical carrier of trace-level metadata.
	if s == t.root {
		priority := t.priority.Load()
		if priority != nil {
			s.setMetricLocked(keySamplingPriority, *priority)
		}
		t.mu.RLock()
		t.setTraceTagsLocked(s)
		t.mu.RUnlock()
		// Lock the sampling priority after the root finishes so the DD sampler
		// cannot override it via resampling.
		t.mu.Lock()
		t.locked = true
		t.mu.Unlock()
	}

	// Mocktracer hook — must run before the span is handed off to the writer.
	if mtr, ok := tr.(interface{ FinishSpan(*Span) }); ok {
		mtr.FinishSpan(s)
	}

	// Submit this span to the writer immediately (send-on-finish model).
	// The writer batches spans and sends them to the agent.
	willSend := decisionKeep == samplingDecision(atomic.LoadUint32((*uint32)(&t.samplingDecision)))
	if concreteTracer, ok := tr.(*tracer); ok {
		concreteTracer.submitChunk(&chunk{spans: []*Span{s}, willSend: willSend, isRoot: s == t.root})
	}

	// Atomic completion detection: when finished == started every span that
	// was pushed has also been finished, so the trace is done.
	t.finished.Add(1)
}

// setPeerService sets the peer.service, _dd.peer.service.source, and _dd.peer.service.remapped_from
// tags as applicable for the given span.
// s.mu must be held for writing.
// +checklocks:s.mu
func setPeerService(s *Span, tc TracerConf) {
	assert.RWMutexLocked(&s.mu)
	// val() is used: only specific non-empty values ("client", "producer") qualify as
	// outbound requests, so an unset and an explicitly-empty spanKind are both correctly
	// treated as non-outbound.
	spanKind, _ := s.meta.Get(ext.SpanKind)
	isOutboundRequest := spanKind == ext.SpanKindClient || spanKind == ext.SpanKindProducer

	if s.meta.Has(ext.PeerService) { // peer.service already set on the span
		s.setMetaLocked(keyPeerServiceSource, ext.PeerService)
	} else if isServerless(tc) {
		// Set peerService only in outbound Lambda requests
		if isOutboundRequest {
			if ps := deriveAWSPeerService(&s.meta); ps != "" {
				s.setMetaLocked(ext.PeerService, ps)
				s.setMetaLocked(keyPeerServiceSource, ext.PeerService)
			} else {
				log.Debug("Unable to set peer.service tag for serverless span %q", s.name)
			}
		}
	} else { // no peer.service currently set
		shouldSetDefaultPeerService := isOutboundRequest && tc.PeerServiceDefaults
		if !shouldSetDefaultPeerService {
			return
		}
		source := setPeerServiceFromSource(s)
		if source == "" {
			log.Debug("No source tag value could be found for span %q, peer.service not set", s.name)
			return
		}
		s.setMetaLocked(keyPeerServiceSource, source)
	}
	// Overwrite existing peer.service value if remapped by the user
	if len(tc.PeerServiceMappings) > 0 {
		ps, _ := s.meta.Get(ext.PeerService)
		if to, ok := tc.PeerServiceMappings[ps]; ok {
			s.setMetaLocked(keyPeerServiceRemappedFrom, ps)
			s.setMetaLocked(ext.PeerService, to)
		}
	}
}

/*
checks if we are in a serverless environment

TODO add checks for Azure functions and other serverless environments
*/
func isServerless(tc TracerConf) bool {
	return tc.isLambdaFunction
}

/*
deriveAWSPeerService returns the host name of the
outbound aws service call based on the span metadata,
or an empty string if it cannot be determined.

The mapping is as follows:
  - eventbridge: events.<region>.amazonaws.com
  - sqs:         sqs.<region>.amazonaws.com
  - sns:         sns.<region>.amazonaws.com
  - kinesis:     kinesis.<region>.amazonaws.com
  - dynamodb:    dynamodb.<region>.amazonaws.com
  - s3:          <bucket>.s3.<region>.amazonaws.com (if Bucket param present)
    s3.<region>.amazonaws.com          (otherwise)
*/
func deriveAWSPeerService(sm *traceinternal.SpanMeta) string {
	service, ok := sm.Get(ext.AWSService)
	if !ok {
		return ""
	}
	region, ok := sm.Get(ext.AWSRegion)
	if !ok {
		return ""
	}

	s := strings.ToLower(service)
	switch s {
	case "s3":
		if bucket, ok := sm.Get(ext.S3BucketName); ok {
			return bucket + ".s3." + region + ".amazonaws.com"
		}
		return "s3." + region + ".amazonaws.com"
	case "eventbridge":
		return "events." + region + ".amazonaws.com"
	case "sqs", "sns", "dynamodb", "kinesis":
		return s + "." + region + ".amazonaws.com"
	}
	return ""
}

// hasMetaKeyLocked checks if a key exists in the span's meta map.
// s.mu must be held for reading.
// +checklocks:s.mu
func (s *Span) hasMetaKeyLocked(tag string) bool {
	assert.RWMutexLocked(&s.mu)
	return s.meta.Has(tag)
}

// setPeerServiceFromSource sets peer.service from the sources determined
// by the tags on the span. It returns the source tag name that it used for
// the peer.service value, or the empty string if no valid source tag was available.
// s.mu must be held for writing.
// +checklocks:s.mu
func setPeerServiceFromSource(s *Span) string {
	assert.RWMutexLocked(&s.mu)
	var sources []string
	useTargetHost := true
	dbSys, _ := s.meta.Get(ext.DBSystem)
	switch {
	// order of the cases and their sources matters here. These are in priority order (highest to lowest)
	case s.hasMetaKeyLocked("aws_service"):
		sources = []string{
			"queuename",
			"topicname",
			"streamname",
			"tablename",
			"bucketname",
		}
	case dbSys == ext.DBSystemCassandra:
		sources = []string{
			ext.CassandraContactPoints,
		}
		useTargetHost = false
	case s.hasMetaKeyLocked(ext.DBSystem):
		sources = []string{
			ext.DBName,
			ext.DBInstance,
		}
	case s.hasMetaKeyLocked(ext.MessagingSystem):
		sources = []string{
			ext.KafkaBootstrapServers,
		}
	case s.hasMetaKeyLocked(ext.RPCSystem):
		sources = []string{
			ext.RPCService,
		}
	}
	// network destination tags will be used as fallback unless there are higher priority sources already set.
	if useTargetHost {
		sources = append(sources, []string{
			ext.NetworkDestinationName,
			ext.PeerHostname,
			ext.TargetHost,
		}...)
	}
	for _, source := range sources {
		if val, ok := s.meta.Get(source); ok {
			s.setMetaLocked(ext.PeerService, val)
			return source
		}
	}
	return ""
}

const hexEncodingDigits = "0123456789abcdef"

// spanIDHexEncoded returns the hex encoded string of the given span ID `u`
// with the given padding.
//
// Code is borrowed from `fmt.fmtInteger` in the standard library.
func spanIDHexEncoded(u uint64, padding int) string {
	// The allocated intbuf with a capacity of 68 bytes
	// is large enough for integer formatting.
	var intbuf [68]byte
	buf := intbuf[0:]
	if padding > 68 {
		buf = make([]byte, padding)
	}
	// Because printing is easier right-to-left: format u into buf, ending at buf[i].
	i := len(buf)
	for u >= 16 {
		i--
		buf[i] = hexEncodingDigits[u&0xF]
		u >>= 4
	}
	i--
	buf[i] = hexEncodingDigits[u]
	for i > 0 && padding > len(buf)-i {
		i--
		buf[i] = '0'
	}
	return string(buf[i:])
}
