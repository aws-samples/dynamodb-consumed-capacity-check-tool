package lib

import (
	"strconv"
)

type item struct {
	Pk   string `json:"pk"`
	Sk   string `json:"sk"`
	Text string `json:"text"`
}

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

type DataFlag struct {
	seqPK bool
	seqSK bool
}

func (p *Params) MakeTestData() []item {
	var count string
	testData := make([]item, *p.MaxRequest, *p.MaxRequest)
	for i := 0; i < *p.MaxRequest; i++ {
		count = strconv.Itoa(i)
		pk, sk := makeTestKeyString(count, *p.PartitionKey, *p.SortKey, *p.SeqPK, *p.SeqSK)
		testData[i] = item{Pk: pk, Sk: sk, Text: makeTestData(*p.TestDataSize)}
	}
	return testData
}

func makeTestKeyString(count string, partitionKey string, sortKey string, seqPK bool, seqSK bool) (string, string) {
	var pk, sk string
	switch {
	case seqSK == true && seqPK == true:
		pk = partitionKey + "_" + count
		sk = sortKey + "_" + count
	case seqSK == true && seqPK == false:
		pk = partitionKey
		sk = sortKey
	case seqSK == false && seqPK == true:
		pk = partitionKey + "_" + count
		sk = sortKey
	case seqSK == false && seqPK == false:
		pk = partitionKey
		sk = sortKey
	}

	return pk, sk
}
