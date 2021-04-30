package main

import (
	"context"
	"crypto/tls"
	"github.com/twitchtv/twirp"
	"golang.org/x/sync/semaphore"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/ivm/lang/go/client"
	pb "github.com/Shopify/ivm/rpc/ivm"
)

//MaxConcurrency determines concurrency level in the test
const MaxConcurrency = 1024

type benchmark struct {
	v       pb.IdentityVaultService
	total   int
	batches []*pb.TokenizeBulkResponse
}

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func getRandomBytes(len int) []byte {
	token := make([]byte, len)
	rand.Read(token) //if err just use 0 values, concurrent safe
	return token
}

func getRandomInt() int64 {
	return random.Int63()
}

func getRandomPIIKind() pb.PII {
	r := getRandomInt()
	switch r % 8 {
	case 0:
		return pb.PII_NAME
	case 1:
		return pb.PII_ADDRESS
	case 2:
		return pb.PII_EMAIL
	case 3:
		return pb.PII_PHONE
	case 4:
		return pb.PII_LATLONG
	case 5:
		return pb.PII_USERAGENT
	case 6:
		return pb.PII_ZIP
	case 7:
		return pb.PII_IP
	}

	return pb.PII_TEXT //unreachable
}

func writeBenchmark(b *benchmark) {
	log.Println("Starting Write New Data...")
	batchSize := client.MaxBatchSize
	totalSeconds := 0.0
	iterations := int(math.Ceil((float64)(b.total) / (float64)(batchSize)))
	batches := make([]*pb.TokenizeBulkRequest, iterations)
	b.batches = make([]*pb.TokenizeBulkResponse, 0, iterations)
	for i := 0; i < iterations; i++ {
		batch := new(pb.TokenizeBulkRequest)
		batch.Requests = make([]*pb.TokenizeRequest, batchSize)
		for j := 0; j < batchSize; j++ {
			pii := getRandomBytes(256)
			affn := &pb.Affiliation{
				Ctrl: &pb.Controller{
					Kind: pb.ControllerKind_SHOPIFY_CTRL,
				},
				DataSubjectId: &pb.Identifier{
					Kind:  pb.IdentifierKind_SHOP_ID,
					Value: strconv.FormatInt(getRandomInt(), 10),
				},
			}
			var m pb.Metadata
			m.Key = "producer"
			m.Values = []string{"kafka_nginx"}
			//add it to batch
			batch.Requests[j] = &pb.TokenizeRequest{
				Affn:     affn,
				Data:     pii,
				Kind:     getRandomPIIKind(),
				Metadata: []*pb.Metadata{&m},
			}
		}
		batches[i] = batch
	}

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(MaxConcurrency)
	start := time.Now()
	var mutex sync.Mutex
	authContext := buildAuthContext()
	for _, batch := range batches {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(bch *pb.TokenizeBulkRequest) {
			defer func() {
				sem.Release(1)
				wg.Done()
			}()
			var bulkResponse *pb.TokenizeBulkResponse
			bulkResponse, err := b.v.Tokenize(authContext, bch)
			if err != nil {
				log.Fatal("bulk tokenize failed:", err)
			}
			mutex.Lock()
			b.batches = append(b.batches, bulkResponse)
			mutex.Unlock()
		}(batch)
	}
	wg.Wait()
	totalSeconds += time.Since(start).Seconds()

	for _, batch := range b.batches {
		for _, item := range batch.Responses {
			if len(item.Error) > 0 || item.Token == nil {
				log.Fatal("item storage failed:", item.Error, " token:", item.Token)
			}
		}
	}
	sum := iterations * batchSize
	log.Printf("Total write requests: %v,Write New Data latency per request:%vms\n\n", sum, (totalSeconds / float64(sum) * 1000))
}

func readBenchmark(b *benchmark) {
	log.Println("Starting Detokenize benchmark...")
	batches := make([]*pb.DetokenizeBulkRequest, len(b.batches))
	sum := 0
	for i, tokBulkResp := range b.batches {
		batch := new(pb.DetokenizeBulkRequest)
		batch.Requests = make([]*pb.DetokenizeRequest, len(tokBulkResp.Responses))
		for j := 0; j < len(tokBulkResp.Responses); j++ {
			sum++
			//add it to batch
			batch.Requests[j] = &pb.DetokenizeRequest{
				Token:    tokBulkResp.Responses[j].Token,
				Metadata: true,
			}
		}
		batches[i] = batch
	}

	var wg sync.WaitGroup
	sem := semaphore.NewWeighted(MaxConcurrency)
	start := time.Now()
	authContext := buildAuthContext()
	for _, batch := range batches {
		sem.Acquire(context.Background(), 1)
		wg.Add(1)
		go func(bch *pb.DetokenizeBulkRequest) {
			defer func() {
				sem.Release(1)
				wg.Done()
			}()
			var bulkResponse *pb.DetokenizeBulkResponse
			bulkResponse, err := b.v.Detokenize(authContext, bch)
			if err != nil {
				log.Fatal("bulk detokenize failed:", err)
			}
			for _, br := range bulkResponse.Responses {
				if br.Data == nil || br.Error != "" {
					log.Fatal("detokenization error:", br)
				}
			}
		}(batch)
	}
	wg.Wait()
	totalSeconds := time.Since(start).Seconds()
	log.Printf("Total read requests: %v,Write New Data latency per request:%vms\n\n", sum, (totalSeconds / float64(sum) * 1000))
}

func buildAuthContext() context.Context {
	//Attach the headers to context
	header := make(http.Header)
	header.Set("Access-Token", "another excellent token which is unbelievably secure!")
	authCTX, err := twirp.WithHTTPRequestHeaders(context.Background(), header)
	if err != nil {
		log.Fatal("auth context creation failed:", err)
	}

	return authCTX
}

func main() {
	var httpClient = &http.Client{
		Timeout: 300 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     time.Second * 200,
			MaxConnsPerHost:     128,
			TLSNextProto:        make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		},
	}

	client := pb.NewIdentityVaultServiceProtobufClient("http://localhost:8000", httpClient)
	log.Println("starting twirptest...")
	benchmark := benchmark{
		v:     client,
		total: 100000,
	}

	writeBenchmark(&benchmark)
	readBenchmark(&benchmark)
}
