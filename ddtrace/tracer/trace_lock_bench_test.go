// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package tracer

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

func newBenchTracer(b *testing.B) *tracer {
	b.Helper()
	trc, err := newTracer(withTransport(newDummyTransport()), withNoopStats())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(trc.Stop)
	return trc
}

func runMixed(b *testing.B, nGoroutines int, pushFn func(), injectFactory func() func()) {
	b.Helper()
	var counter atomic.Int64
	total := int64(b.N)
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := range nGoroutines {
		wg.Add(1)
		isInjector := i%2 != 0
		go func() {
			defer wg.Done()
			var localInject func()
			if isInjector {
				localInject = injectFactory()
			}
			for {
				if counter.Add(1) > total {
					return
				}
				if isInjector {
					localInject()
				} else {
					pushFn()
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkTracePushOnly measures span creation (push) with N goroutines.
func BenchmarkTracePushOnly(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("goroutines_%04d", n), func(b *testing.B) {
			trc := newBenchTracer(b)
			root := trc.StartSpan("grpc.server")
			ctx := root.Context()
			var counter atomic.Int64
			total := int64(b.N)
			var wg sync.WaitGroup
			b.ReportAllocs()
			b.ResetTimer()
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						if counter.Add(1) > total {
							return
						}
						child := trc.StartSpan("grpc.client", ChildOf(ctx))
						child.Finish()
					}
				}()
			}
			wg.Wait()
			root.Finish()
		})
	}
}

// BenchmarkTraceInjectOnly measures inject (header propagation) with N goroutines.
func BenchmarkTraceInjectOnly(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("goroutines_%04d", n), func(b *testing.B) {
			trc := newBenchTracer(b)
			root := trc.StartSpan("grpc.server")
			ctx := root.Context()
			var counter atomic.Int64
			total := int64(b.N)
			var wg sync.WaitGroup
			b.ReportAllocs()
			b.ResetTimer()
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					carrier := HTTPHeadersCarrier(http.Header{})
					for {
						if counter.Add(1) > total {
							return
						}
						_ = trc.Inject(ctx, carrier)
					}
				}()
			}
			wg.Wait()
			root.Finish()
		})
	}
}

// BenchmarkTracePushInjectContention: push vs inject racing on the same trace.
func BenchmarkTracePushInjectContention(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("goroutines_%04d", n), func(b *testing.B) {
			trc := newBenchTracer(b)
			root := trc.StartSpan("grpc.server")
			ctx := root.Context()
			b.ReportAllocs()
			runMixed(b, n,
				func() {
					child := trc.StartSpan("grpc.client", ChildOf(ctx))
					_ = child
				},
				func() func() {
					carrier := HTTPHeadersCarrier(http.Header{})
					return func() { _ = trc.Inject(ctx, carrier) }
				},
			)
			root.Finish()
		})
	}
}

// BenchmarkTraceMutexContention: full lifecycle (push+finish) mixed with inject.
func BenchmarkTraceMutexContention(b *testing.B) {
	for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("goroutines_%04d", n), func(b *testing.B) {
			trc := newBenchTracer(b)
			root := trc.StartSpan("grpc.server")
			ctx := root.Context()
			b.ReportAllocs()
			runMixed(b, n,
				func() {
					child := trc.StartSpan("grpc.client", ChildOf(ctx))
					child.Finish()
				},
				func() func() {
					carrier := HTTPHeadersCarrier(http.Header{})
					return func() { _ = trc.Inject(ctx, carrier) }
				},
			)
			root.Finish()
		})
	}
}
