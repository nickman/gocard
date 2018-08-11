package prometheus

import (
	pr "github.com/prometheus/client_golang/prometheus"
	"hash"
	"bytes"
	"github.com/prometheus/client_model/go"
	"github.com/clarkduvall/hyperloglog"
	"sync"
	"github.com/OneOfOne/xxhash"
)

var (
	hashPool	sync.Pool
)

const (
	HASHPOOL_SIZE = 128
)

func init() {
	hashPool = sync.Pool{
		New: func() interface{} {
			return xxhash.New64()
		},
	}
	arr := make([]interface{}, HASHPOOL_SIZE)
	for i := 0; i < HASHPOOL_SIZE; i++ {
		arr[i] = hashPool.Get()
	}
	for i := 0; i < HASHPOOL_SIZE; i++ {
		hashPool.Put(arr[i])
	}
}

type Cardinality interface {
	pr.Metric
	pr.Collector

	WriteHash(hash64 hash.Hash64)
	WriteString(s string)
	WriteBytes(b []byte)
	WriteStream(buff bytes.Buffer)
	Count() uint64
	Clear() uint64
}

type CardinalityOpts struct {
	pr.GaugeOpts
	Precision 		uint8
}

type card struct {
	Cardinality
	gauge 		pr.Gauge
	opts		CardinalityOpts
	hlog 		*hyperloglog.HyperLogLogPlus
	lock 		*sync.Mutex

}

func NewCardinality(copts CardinalityOpts) Cardinality {
	h,_ := hyperloglog.NewPlus(copts.Precision)
	c := card{
		opts:copts,
		gauge:			pr.NewGauge(pr.GaugeOpts{
			ConstLabels:		copts.ConstLabels,
			Help: 				copts.Help,
			Name: 				copts.Name,
			Namespace: 			copts.Namespace,
			Subsystem: 			copts.Subsystem,
		}),
		hlog: 			h,
		lock: 			new(sync.Mutex),
	}
	return &c
}



func (c *card) Desc() *pr.Desc {
	return c.gauge.Desc()
}

func (c *card) Write(m *io_prometheus_client.Metric) error {
	c.Clear()
	return c.gauge.Write(m)
}

func (c *card) Describe(ch chan<- *pr.Desc) {
	c.gauge.Describe(ch)
}

func (c *card) Collect(ch chan<- pr.Metric) {
	c.gauge.Collect(ch)
}

func (c *card) WriteHash(hash64 hash.Hash64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.hlog.Add(hash64)
	c.gauge.Set(float64(c.hlog.Count()))
}

func (c *card) WriteString(s string) {
	hasher := getHasher()
	defer closeHasher(hasher)
	hasher.WriteString(s)
	c.WriteHash(hasher)
}

func (c *card) WriteBytes(b []byte) {
	hasher := getHasher()
	defer closeHasher(hasher)
	hasher.Write(b)
	c.WriteHash(hasher)
}

func (c *card) WriteStream(buff bytes.Buffer) {
	hasher := getHasher()
	defer closeHasher(hasher)
	hasher.Write(buff.Bytes())
	c.WriteHash(hasher)
}

func (c *card) Count() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.hlog.Count()
}

func (c *card) Clear() uint64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	cnt := c.hlog.Count()
	c.hlog.Clear()
	return cnt
}

func getHasher() *xxhash.XXHash64 {

	h := hashPool.Get().(*xxhash.XXHash64)
	h.Reset()
	return h
}

func closeHasher(hasher *xxhash.XXHash64) {
	hashPool.Put(hasher)
}

