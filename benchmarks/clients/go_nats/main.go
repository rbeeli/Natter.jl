package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

func percentile(values []float64, q float64) float64 {
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	idx := int(float64(len(cp)-1)*q + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func waitFor(fn func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		runtime.Gosched()
	}
	return fmt.Errorf("timed out waiting for benchmark condition")
}

func timedRepeated(unitCount int, minSeconds float64, runRound func() error) (int, int, float64, error) {
	units := 0
	rounds := 0
	runtime.GC()
	started := time.Now()
	for {
		if err := runRound(); err != nil {
			return 0, 0, 0, err
		}
		units += unitCount
		rounds++
		seconds := time.Since(started).Seconds()
		if seconds >= minSeconds {
			return units, rounds, seconds, nil
		}
	}
}

func connect(url string) (*nats.Conn, error) {
	return nats.Connect(url, nats.Timeout(2*time.Second), nats.NoReconnect())
}

func benchPublishBatch(url, subject string, payload []byte, messages int, timeout time.Duration, minSeconds float64) (map[string]any, error) {
	nc, err := connect(url)
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	warmup := min(messages, 1000)
	for i := 0; i < warmup; i++ {
		if err := nc.Publish(subject, payload); err != nil {
			return nil, err
		}
	}
	if err := nc.FlushTimeout(timeout); err != nil {
		return nil, err
	}

	totalMessages, rounds, seconds, err := timedRepeated(messages, minSeconds, func() error {
		for i := 0; i < messages; i++ {
			if err := nc.Publish(subject, payload); err != nil {
				return err
			}
		}
		return nc.FlushTimeout(timeout)
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"messages":               totalMessages,
		"configured_messages":    messages,
		"rounds":                 rounds,
		"seconds":                seconds,
		"messages_per_second":    float64(totalMessages) / seconds,
		"payload_mib_per_second": float64(totalMessages*len(payload)) / seconds / 1024.0 / 1024.0,
	}, nil
}

func benchPublishFlushEach(url, subject string, payload []byte, messages int, timeout time.Duration, minSeconds float64) (map[string]any, error) {
	nc, err := connect(url)
	if err != nil {
		return nil, err
	}
	defer nc.Close()

	warmup := min(messages, 100)
	for i := 0; i < warmup; i++ {
		if err := nc.Publish(subject, payload); err != nil {
			return nil, err
		}
		if err := nc.FlushTimeout(timeout); err != nil {
			return nil, err
		}
	}

	totalMessages, rounds, seconds, err := timedRepeated(messages, minSeconds, func() error {
		for i := 0; i < messages; i++ {
			if err := nc.Publish(subject, payload); err != nil {
				return err
			}
			if err := nc.FlushTimeout(timeout); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"messages":               totalMessages,
		"configured_messages":    messages,
		"rounds":                 rounds,
		"seconds":                seconds,
		"messages_per_second":    float64(totalMessages) / seconds,
		"payload_mib_per_second": float64(totalMessages*len(payload)) / seconds / 1024.0 / 1024.0,
	}, nil
}

func benchCallbackDispatch(url, subject string, payload []byte, messages int, timeout time.Duration, minSeconds float64) (map[string]any, error) {
	sub, err := connect(url)
	if err != nil {
		return nil, err
	}
	defer sub.Close()
	pub, err := connect(url)
	if err != nil {
		return nil, err
	}
	defer pub.Close()

	var received atomic.Int64
	if _, err := sub.Subscribe(subject, func(_ *nats.Msg) {
		received.Add(1)
	}); err != nil {
		return nil, err
	}
	if err := sub.FlushTimeout(timeout); err != nil {
		return nil, err
	}

	warmup := min(messages, 1000)
	for i := 0; i < warmup; i++ {
		if err := pub.Publish(subject, payload); err != nil {
			return nil, err
		}
	}
	if err := pub.FlushTimeout(timeout); err != nil {
		return nil, err
	}
	if err := waitFor(func() bool { return received.Load() >= int64(warmup) }, timeout); err != nil {
		return nil, err
	}
	target := received.Load()

	totalMessages, rounds, seconds, err := timedRepeated(messages, minSeconds, func() error {
		target += int64(messages)
		for i := 0; i < messages; i++ {
			if err := pub.Publish(subject, payload); err != nil {
				return err
			}
		}
		if err := pub.FlushTimeout(timeout); err != nil {
			return err
		}
		return waitFor(func() bool { return received.Load() >= target }, timeout)
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"messages":            totalMessages,
		"configured_messages": messages,
		"received":            totalMessages,
		"rounds":              rounds,
		"seconds":             seconds,
		"messages_per_second": float64(totalMessages) / seconds,
	}, nil
}

func benchRequestReply(url, subject string, payload []byte, requests int, timeout time.Duration, minSeconds float64) (map[string]any, error) {
	service, err := connect(url)
	if err != nil {
		return nil, err
	}
	defer service.Close()
	client, err := connect(url)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if _, err := service.Subscribe(subject, func(msg *nats.Msg) {
		_ = msg.Respond(payload)
	}); err != nil {
		return nil, err
	}
	if err := service.FlushTimeout(timeout); err != nil {
		return nil, err
	}

	warmup := min(requests, 200)
	for i := 0; i < warmup; i++ {
		if _, err := client.Request(subject, payload, timeout); err != nil {
			return nil, err
		}
	}

	latencies := make([]float64, 0, requests)
	totalRequests, rounds, seconds, err := timedRepeated(requests, minSeconds, func() error {
		for i := 0; i < requests; i++ {
			t := time.Now()
			if _, err := client.Request(subject, payload, timeout); err != nil {
				return err
			}
			latencies = append(latencies, float64(time.Since(t).Nanoseconds())/1e6)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	var sum float64
	var max float64
	for _, v := range latencies {
		sum += v
		if v > max {
			max = v
		}
	}
	return map[string]any{
		"requests":            totalRequests,
		"configured_requests": requests,
		"rounds":              rounds,
		"seconds":             seconds,
		"requests_per_second": float64(totalRequests) / seconds,
		"latency_ms_mean":     sum / float64(len(latencies)),
		"latency_ms_p50":      percentile(latencies, 0.50),
		"latency_ms_p95":      percentile(latencies, 0.95),
		"latency_ms_p99":      percentile(latencies, 0.99),
		"latency_ms_max":      max,
	}, nil
}

func main() {
	url := flag.String("url", "nats://127.0.0.1:4222", "")
	messages := flag.Int("messages", 200000, "")
	requests := flag.Int("requests", 20000, "")
	payloadBytes := flag.Int("payload-bytes", 64, "")
	timeoutSeconds := flag.Float64("timeout", 90, "")
	minSeconds := flag.Float64("min-seconds", 5.0, "")
	jsonPath := flag.String("json", "/tmp/go-nats.json", "")
	flag.Parse()
	if *minSeconds <= 0 {
		panic("-min-seconds must be positive")
	}

	timeout := time.Duration(*timeoutSeconds * float64(time.Second))
	payload := make([]byte, *payloadBytes)
	for i := range payload {
		payload[i] = 'x'
	}
	prefix := fmt.Sprintf("go.perf.%d", time.Now().UnixNano())
	benchmarks := map[string]any{}
	var err error
	if benchmarks["publish_batch"], err = benchPublishBatch(*url, prefix+".publish.batch", payload, *messages, timeout, *minSeconds); err != nil {
		panic(err)
	}
	if benchmarks["publish_flush_each"], err = benchPublishFlushEach(*url, prefix+".publish.flush", payload, *messages, timeout, *minSeconds); err != nil {
		panic(err)
	}
	if benchmarks["callback_dispatch"], err = benchCallbackDispatch(*url, prefix+".callback", payload, *messages, timeout, *minSeconds); err != nil {
		panic(err)
	}
	if benchmarks["request_reply"], err = benchRequestReply(*url, prefix+".request", payload, *requests, timeout, *minSeconds); err != nil {
		panic(err)
	}

	report := map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"environment": map[string]string{
			"client":          "nats.go",
			"go_version":      runtime.Version(),
			"url":             *url,
			"messages":        fmt.Sprint(*messages),
			"requests":        fmt.Sprint(*requests),
			"payload_bytes":   fmt.Sprint(*payloadBytes),
			"min_seconds":     fmt.Sprint(*minSeconds),
			"flush_semantics": "server_round_trip",
		},
		"benchmarks": benchmarks,
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(*jsonPath, append(data, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Println(string(data))
}
