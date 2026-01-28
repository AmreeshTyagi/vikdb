// vikdb-bench is a load generator for VikDB HTTP API.
// Run VikDB first (e.g. make run), then: go run ./cmd/vikdb-bench -addr localhost:8080 -rps 500 -duration 10s
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "VikDB HTTP address (e.g. localhost:8080)")
	rps := flag.Int("rps", 500, "Target requests per second")
	duration := flag.Duration("duration", 10*time.Second, "Benchmark duration (e.g. 10s, 1m)")
	concurrency := flag.Int("concurrency", 5, "Number of worker goroutines")

	putPct := flag.Int("put", 100, "Put ratio 0-100 (default 100 = put-only)")
	readPct := flag.Int("read", 0, "Read ratio 0-100")
	rangePct := flag.Int("range", 0, "ReadKeyRange ratio 0-100")
	batchPct := flag.Int("batch", 0, "BatchPut ratio 0-100")
	deletePct := flag.Int("delete", 0, "Delete ratio 0-100")

	flag.Parse()

	total := *putPct + *readPct + *rangePct + *batchPct + *deletePct
	if total == 0 {
		fmt.Println("At least one of -put, -read, -range, -batch, -delete must be > 0")
		return
	}

	baseURL := "http://" + *addr
	if len(*addr) > 0 && (*addr)[0] == ':' {
		baseURL = "http://localhost" + *addr
	}

	client := &http.Client{Timeout: 30 * time.Second}

	// Rate limiter: one token every 1/rps seconds, stops when benchmark ends
	limiter := make(chan struct{}, *rps*2)
	stopLimiter := make(chan struct{})
	go func() {
		interval := time.Second / time.Duration(*rps)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopLimiter:
				return
			case <-ticker.C:
				select {
				case limiter <- struct{}{}:
				default:
				}
			}
		}
	}()

	var (
		latencies   []time.Duration
		latMu       sync.Mutex
		totalOps    int64
		totalErrors int64
		opCounts    = make(map[string]int64)
		opMu        sync.Mutex
	)

	keySpace := 100_000
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	start := time.Now()
	deadline := start.Add(*duration)
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLats := make([]time.Duration, 0, 4096)
			for time.Now().Before(deadline) {
				<-limiter
				op := pickOp(rng, *putPct, *readPct, *rangePct, *batchPct, *deletePct, total)
				t0 := time.Now()
				err := doOp(client, baseURL, op, keySpace, rng)
				elapsed := time.Since(t0)
				localLats = append(localLats, elapsed)
				atomic.AddInt64(&totalOps, 1)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
				}
				opMu.Lock()
				opCounts[op]++
				opMu.Unlock()
			}
			latMu.Lock()
			latencies = append(latencies, localLats...)
			latMu.Unlock()
		}()
	}
	wg.Wait()
	close(stopLimiter)
	elapsed := time.Since(start)

	// Report
	actualRPS := float64(totalOps) / elapsed.Seconds()
	fmt.Println("--- VikDB benchmark ---")
	fmt.Printf("  Target RPS:     %d\n", *rps)
	fmt.Printf("  Duration:       %s\n", duration)
	fmt.Printf("  Concurrency:   %d\n", *concurrency)
	fmt.Printf("  Total requests:%d\n", totalOps)
	fmt.Printf("  Actual RPS:    %.1f\n", actualRPS)
	fmt.Printf("  Errors:        %d\n", totalErrors)
	fmt.Printf("  Op mix: put=%d read=%d range=%d batch=%d delete=%d\n", *putPct, *readPct, *rangePct, *batchPct, *deletePct)
	opMu.Lock()
	for op, n := range opCounts {
		fmt.Printf("  %s: %d\n", op, n)
	}
	opMu.Unlock()

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		fmt.Printf("  Latency p50:    %s\n", p50)
		fmt.Printf("  Latency p95:    %s\n", p95)
		fmt.Printf("  Latency p99:    %s\n", p99)
	}
}

func pickOp(rng *rand.Rand, put, read, range_, batch, delete, total int) string {
	x := rng.Intn(total)
	if x < put {
		return "put"
	}
	x -= put
	if x < read {
		return "read"
	}
	x -= read
	if x < range_ {
		return "range"
	}
	x -= range_
	if x < batch {
		return "batch"
	}
	return "delete"
}

func doOp(client *http.Client, baseURL, op string, keySpace int, rng *rand.Rand) error {
	switch op {
	case "put":
		return doPut(client, baseURL, keySpace, rng)
	case "read":
		return doRead(client, baseURL, keySpace, rng)
	case "range":
		return doRange(client, baseURL, keySpace, rng)
	case "batch":
		return doBatch(client, baseURL, keySpace, rng)
	case "delete":
		return doDelete(client, baseURL, keySpace, rng)
	}
	return nil
}

func keyB64(k int) string {
	key := fmt.Sprintf("key-%08d", k)
	return base64.URLEncoding.EncodeToString([]byte(key))
}

func valueB64(k int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("value-%d", k)))
}

func doPut(client *http.Client, baseURL string, keySpace int, rng *rand.Rand) error {
	k := 1 + rng.Intn(keySpace)
	keyEnc := keyB64(k)
	valEnc := valueB64(k)
	body, _ := json.Marshal(map[string]string{"value": valEnc})
	req, err := http.NewRequest(http.MethodPut, baseURL+"/kv/"+keyEnc, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTemporaryRedirect {
		return fmt.Errorf("put status %d", resp.StatusCode)
	}
	return nil
}

func doRead(client *http.Client, baseURL string, keySpace int, rng *rand.Rand) error {
	k := 1 + rng.Intn(keySpace)
	keyEnc := keyB64(k)
	resp, err := client.Get(baseURL + "/kv/" + keyEnc)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusTemporaryRedirect {
		return fmt.Errorf("read status %d", resp.StatusCode)
	}
	return nil
}

func doRange(client *http.Client, baseURL string, keySpace int, rng *rand.Rand) error {
	startK := 1 + rng.Intn(keySpace-10)
	if startK < 1 {
		startK = 1
	}
	endK := startK + 1 + rng.Intn(100)
	if endK > keySpace {
		endK = keySpace
	}
	startEnc := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("key-%08d", startK)))
	endEnc := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("key-%08d", endK)))
	rangeURL := baseURL + "/kv/range?start=" + url.QueryEscape(startEnc) + "&end=" + url.QueryEscape(endEnc)
	resp, err := client.Get(rangeURL)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTemporaryRedirect {
		return fmt.Errorf("range status %d", resp.StatusCode)
	}
	return nil
}

func doBatch(client *http.Client, baseURL string, keySpace int, rng *rand.Rand) error {
	size := 2 + rng.Intn(9) // 2–10 keys per batch
	keys := make([]string, size)
	vals := make([]string, size)
	for i := 0; i < size; i++ {
		k := 1 + rng.Intn(keySpace)
		keys[i] = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("key-%08d", k)))
		vals[i] = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("value-%d", k)))
	}
	body, _ := json.Marshal(map[string]interface{}{"keys": keys, "values": vals})
	req, err := http.NewRequest(http.MethodPost, baseURL+"/kv/batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTemporaryRedirect {
		return fmt.Errorf("batch status %d", resp.StatusCode)
	}
	return nil
}

func doDelete(client *http.Client, baseURL string, keySpace int, rng *rand.Rand) error {
	k := 1 + rng.Intn(keySpace)
	keyEnc := keyB64(k)
	req, err := http.NewRequest(http.MethodDelete, baseURL+"/kv/"+keyEnc, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTemporaryRedirect {
		return fmt.Errorf("delete status %d", resp.StatusCode)
	}
	return nil
}
