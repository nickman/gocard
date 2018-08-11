package prometheus

import (
	"testing"
	"math/rand"
	pr "github.com/prometheus/client_golang/prometheus"
	"fmt"
	"unsafe"
	"math"
	"github.com/prometheus/client_model/go"
	"sync"
	"time"
)

const (
	VARIANCE = float64(0.01)
)

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

)

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestRandomCardSingleRoutine(t *testing.T) {
	testRandom(t, 20, 100, 16, 1)
	testRandom(t, 20, 1000, 16, 1)
	testRandom(t, 20, 10000, 16, 1)
	testRandom(t, 20, 100000, 16, 1)
	testRandom(t, 200, 100, 16, 1)
	testRandom(t, 200, 1000, 16, 1)
	testRandom(t, 200, 10000, 16, 1)
	testRandom(t, 200, 100000, 16, 1)
	testRandom(t, 20, 100, 16, 4)
	testRandom(t, 20, 1000, 16, 4)
	testRandom(t, 20, 10000, 16, 4)
	testRandom(t, 200, 100000, 16, 4)

}

func randomArr(stringsize, samples int) []string {
	m := make(map[string]string, samples)
	for {
		rs := randomString(stringsize)
		m[rs] = ""
		if len(m) == samples {
			break
		}
	}
	arr := make([]string, samples)
	idx := 0
	for key := range m {
		arr[idx] = key
		idx++
	}
	m = nil
	return arr
}

func testRandom(t *testing.T, stringsize, samples, precision , concurrency int) {
	timer := pr.NewSummary(pr.SummaryOpts{
		Help: 	"Timer Help",
		Name: 	"Timer",
	})
	pr.MustRegister(timer)
	defer pr.Unregister(timer)
	desc := fmt.Sprintf("Cardinality Test: samplesize=%d, samplecount=%d, precision=%d, concurrency=%d", stringsize,samples,precision, concurrency)
	card := NewCardinality(CardinalityOpts{
		Precision: 	uint8(precision),
		GaugeOpts:	pr.GaugeOpts{},
	})
	m := randomArr(stringsize, samples)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	each := samples/concurrency
	start := time.Now()
	for r := 0; r < concurrency; r++ {
		startAt := r*each
		endAt := startAt + each
		myrange := m[startAt:endAt]
		go func() {
			for _,k := range myrange {
				start := time.Now()
				card.WriteString(k)
				timer.Observe(time.Since(start).Seconds())
			}
			wg.Done()
		}()
	}
	wg.Wait()
	timeMetric := &io_prometheus_client.Metric{}
	timer.Write(timeMetric)

	elapsed := time.Since(start).Seconds()
	memSize := unsafe.Sizeof(card)
	samples64 := uint64(samples)
	size := card.Count()
	if size != samples64 {
		sdiff := int(size) - int(samples64)
		diff := math.Abs(float64(sdiff))
		if diff != float64(0) {
			variance := diff/float64(size)
			if variance > VARIANCE {
				t.Errorf("Failed card test: %s, Expected %d, but got %d. diff=%f, variance: %f", desc, samples, size, diff, variance)
			} else {
				fmt.Printf("Variance ok: diff=%f, variance: %f\n", diff, variance)
			}

		}
	}
	outMetric := &io_prometheus_client.Metric{}
	card.Write(outMetric)
	//per := elapsed/float64(samples)*float64(concurrency)*float64(1000)
	qtiles := timeMetric.Summary.Quantile

	fmt.Printf("%s, elapsed: %f sec., %s, cardcount: %d, memsize: %d bytes, gauge: %f\n", desc, elapsed, printSummary(qtiles), size, memSize, *outMetric.Gauge.Value)

	if card.Count() != 0 {
		t.Errorf("Card did not clear. Expected count: 0, but got %d", card.Count())
	}

}


func printSummary(quantiles []*io_prometheus_client.Quantile) string {
	top := float64(100)
	toms := float64(1000)
	str := "["
	for _,q := range quantiles {
		key := fmt.Sprintf("p%d: %f ms.,", int32(*q.Quantile*top), *q.Value*toms)
		str += key
	}
	str += "]"
	return str
}