package lib

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-dax-go/dax"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func makeTestData(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func PutData(allReq *uint32, successReq *uint32, errorReq *uint32, maxRequest int, dataSize int,
	tableName string, awsRegion string, enableDax bool, DAXEndpoint string, testData []item, threadStartIndex int,
	threadCount int) (putConsumedCapacity float64) {
	fmt.Printf("PUT start child execute : time:%v,Counter:%v,MaxRequets:%v\n",
		time.Now(), atomic.LoadUint32(allReq), maxRequest)

	sess, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})

	if err != nil {
		fmt.Println("PUT Got session error:", err.Error())
		os.Exit(1)
	}
	svc := dynamodb.New(sess, aws.NewConfig().WithMaxRetries(0))

	//svc := createDAXClinet(awsRegion)

	var grCount int

	fmt.Println("PUT This goroutine  threadStartIndex:", threadStartIndex, "threadCount:", threadCount)
	for i := threadStartIndex; i < threadStartIndex+threadCount; i++ {
		item := testData[i]
		putData := makePutTestData(tableName, item.Pk, item.Sk, makeTestData(dataSize))
		result, err := svc.PutItem(putData)

		if result.ConsumedCapacity.CapacityUnits != nil {
			putConsumedCapacity = putConsumedCapacity + *result.ConsumedCapacity.CapacityUnits
		}
		if err != nil {
			fmt.Printf("putItem error: %v\n", err.Error())
			atomic.AddUint32(errorReq, 1)
		} else {
			atomic.AddUint32(successReq, 1)
		}
		atomic.AddUint32(allReq, 1)
		grCount = grCount + 1
	}

	fmt.Printf("PUT end child execute:time:%v,Counter:%v,consumed capacity:%v\n", time.Now(), grCount, putConsumedCapacity)

	return putConsumedCapacity
}

func makePutTestData(tableName string, partitionKey string, sortKey string, testText string) (putItem *dynamodb.PutItemInput) {
	testData := item{
		Pk:   partitionKey,
		Sk:   sortKey,
		Text: testText,
	}

	av, err := dynamodbattribute.MarshalMap(testData)

	if err != nil {
		fmt.Println("PUT Got error marshalling map:")
		fmt.Println(err.Error())
		os.Exit(1)
	}

	benchmarkItem := &dynamodb.PutItemInput{
		Item:                   av,
		TableName:              aws.String(tableName),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	return benchmarkItem
}

func GetData(allReq *uint32, successReq *uint32, errorReq *uint32, maxRequest int,
	tableName string, awsRegion string, testData []item, threadStartIndex int, threadCount int) (getConsumedCapacity float64) {
	fmt.Printf("GET start child execute : time:%v,Counter:%v,MaxRequets:%v\n",
		time.Now(), atomic.LoadUint32(allReq), maxRequest)

	sess, err := session.NewSession(&aws.Config{Region: aws.String(awsRegion)})
	if err != nil {
		fmt.Println("GET Got session error:", err.Error())
		os.Exit(1)
	}
	svc := dynamodb.New(sess, aws.NewConfig().WithMaxRetries(0))

	//svc := createDAXClinet(awsRegion)

	var grCount int

	fmt.Println("GET This goroutine  threadStartIndex:", threadStartIndex, "threadCount:", threadCount)
	for i := threadStartIndex; i < threadStartIndex+threadCount; i++ {
		item := testData[i]
		getData := makeGetTestData(tableName, item.Pk, item.Sk)
		result, err := svc.GetItem(getData)
		if result.ConsumedCapacity.CapacityUnits != nil {
			getConsumedCapacity = getConsumedCapacity + *result.ConsumedCapacity.CapacityUnits
		}
		if err != nil {
			fmt.Printf("GET : getItem error: %v\n", err.Error())
			atomic.AddUint32(errorReq, 1)
		} else {
			atomic.AddUint32(successReq, 1)
		}
		atomic.AddUint32(allReq, 1)
		grCount = grCount + 1
	}

	fmt.Printf("GET end child execute:time:%v,Counter:%v,consumed capacity:%v\n", time.Now(), grCount, getConsumedCapacity)

	return getConsumedCapacity
}

func makeGetTestData(tableName string, partitionKey string, sortKey string) (getItem *dynamodb.GetItemInput) {
	benchmarkItem := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"pk": {
				S: aws.String(partitionKey),
			},
			"sk": {
				S: aws.String(sortKey),
			},
		},
		TableName:              aws.String(tableName),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	return benchmarkItem
}

func AddFloat64(val *float64, delta float64) (new float64) {
	for {
		old := *val
		new = old + delta
		if atomic.CompareAndSwapUint64(
			(*uint64)(unsafe.Pointer(val)),
			math.Float64bits(old),
			math.Float64bits(new),
		) {
			break
		}
	}
	return
}

func createDAXClinet(awsRegion string) (svc *dax.Dax) {
	cfg := dax.DefaultConfig()
	cfg.ReadRetries = 0
	cfg.WriteRetries = 0
	cfg.MaxPendingConnectionsPerHost = 100
	cfg.HostPorts = []string{"dax://stress.ab0uwo.dax-clusters.ap-northeast-1.amazonaws.com:8111"}
	cfg.Region = awsRegion
	svc, _ = dax.New(cfg)
	return svc
}
