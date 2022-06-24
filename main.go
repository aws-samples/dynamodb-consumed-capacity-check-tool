package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws-samples/dynamodb-consumed-capacity-check-tool/lib"
)

type Params struct {
	TableName    *string
	ConcurrentNo *int
	MaxRequest   *int
	PartitionKey *string
	SortKey      *string
	TestDataSize *int
	AwsRegion    *string
	Workload     *string
	SeqPK        *bool
	SeqSK        *bool
	EnableDax    *bool
	DAXEndpoint  *string
}

func main() {
	var params = initParams()
	printParams(params)

	testDataGenerator := lib.Params(params)
	testData := testDataGenerator.MakeTestData()

	startTime := time.Now()
	fmt.Printf("Start concurrency execute %s\n", startTime)
	runtime.GOMAXPROCS(runtime.NumCPU())

	var allReq, successReq, errorReq uint32
	var putConsumedCapacity, getConsumedCapacity uint64

	ticker(&allReq, &successReq, &errorReq)
	threadCount := len(testData) / *params.ConcurrentNo

	fmt.Printf("All test request:%v,Concurrent:%v,1thread count:%v\n",
		len(testData), *params.ConcurrentNo, threadCount)

	wg := sync.WaitGroup{}
	for i := 0; i < *params.ConcurrentNo; i++ {
		threadStartIndex := 0
		wg.Add(1)
		go func(i int) {
			if i != 0 {
				threadStartIndex = threadCount * i
			}
			if *params.Workload == "all" || *params.Workload == "write" {
				fmt.Printf("PUT benchmark test start\n")
				putResult := lib.PutData(&allReq, &successReq, &errorReq, *params.MaxRequest, *params.TestDataSize,
					*params.TableName, *params.AwsRegion, *params.EnableDax, *params.DAXEndpoint, testData,
					threadStartIndex,
					threadCount)
				atomic.AddUint64(&putConsumedCapacity, uint64(putResult))
			}

			if *params.Workload == "all" || *params.Workload == "read" {
				fmt.Printf("GET benchmark test start\n")
				getResult := lib.GetData(&allReq, &successReq, &errorReq, *params.MaxRequest,
					*params.TableName, *params.AwsRegion, *params.EnableDax, *params.DAXEndpoint, testData,
					threadStartIndex, threadCount)

				fmt.Println("get consumed capacity is ", getResult)
				atomic.AddUint64(&getConsumedCapacity, uint64(getResult))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	endTime := time.Now()
	benchmarkTime := endTime.Sub(startTime)
	throughput := float64(allReq) / benchmarkTime.Seconds()

	fmt.Printf("End all execute.\nBenchmark start time:%s\nBenchmark end time:%s\n"+
		"All request count:%d,success:%d,error%d\n",
		startTime, endTime, allReq, successReq, errorReq)

	fmt.Printf("Benchmark total time:%vsec\nAvg throughput:%vreq/sec\n",
		benchmarkTime.Seconds(), throughput)
	// Get consumed capacity is float value. but there isn't atomic float type.
	getConsumedCapacity = getConsumedCapacity / 10

	fmt.Printf("Benchmark total consumend capacity\nPUT:%v\nGET:%v (RoundDown)\n",
		putConsumedCapacity, getConsumedCapacity)

	printParams(params)

	os.Exit(0)

}

func initParams() Params {
	var params Params
	params.TableName = flag.String("table", "benchmark", "Use DynamoDB table name")
	params.EnableDax = flag.Bool("dax", false, "Enable DAX Cluster")
	params.DAXEndpoint = flag.String("daxep", "dax://sampleEndpoint", "DAX Endpoint")
	params.ConcurrentNo = flag.Int("con", 2, "Concurrent Request No")
	params.MaxRequest = flag.Int("max", 100, "max Request count")
	params.PartitionKey = flag.String("pk", "pk", "partition key string")
	params.SortKey = flag.String("sk", "sk", "sort key string")
	params.TestDataSize = flag.Int("datasize", 16, "test data byte size")
	params.AwsRegion = flag.String("region", "ap-northeast-1", "Use aws region")
	params.Workload = flag.String("workload", "all", "workload : all -> read&write, write -> write only, "+
		"read -> read only")
	params.SeqPK = flag.Bool("seqpk", false, "bool:generate sequence pk data(ex:PK_1,PK_2...) or fix string")
	params.SeqSK = flag.Bool("seqsk", false, "bool:generate sequence sk data(ex:SK_1,SK_2...) or fix string")

	flag.Parse()

	//fmt.Println("Init params", *params.SeqPK, *params.SeqSK)

	return params
}

func ticker(allReq *uint32, successReq *uint32, errorReq *uint32) {
	go func() {
		t := time.NewTicker(time.Second)
		for {
			select {
			case now := <-t.C:
				printCounter(now, allReq, successReq, errorReq)
			}
		}
		t.Stop()
	}()
}

func printCounter(now time.Time, allReq *uint32, successReq *uint32, errorReq *uint32) {
	fmt.Printf("Benchmarking..... time:%s,All request count:%v,success:%v,error:%v\n",
		now.Format("2006-01-02 15-04-05.000"), atomic.LoadUint32(allReq),
		atomic.LoadUint32(successReq), atomic.LoadUint32(errorReq))
}

func printParams(params Params) {
	fmt.Printf("Set params tableName:%v concurrentNo:%v maxRequest:%v"+
		" partitionKey:%v sortKey:%v awsRegion:%v workload:%v daxEndpoint:%v generatePK:%v generateSK:%v\n",
		*params.TableName, *params.ConcurrentNo, *params.MaxRequest, *params.PartitionKey,
		*params.SortKey, *params.AwsRegion, *params.Workload, *params.DAXEndpoint, *params.SeqPK, *params.SeqSK)
}
