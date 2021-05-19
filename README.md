# dynamodb-simple-benchmark

How to Use
This tool needs AWS credentials or DynamoDB PUT/GET permission role. Check the authentication method when using the AWS SDK, if necessary.

```
git clone https://github.com/oranie/dynamodb-simple-benchmark.git
cd ./dynamodb-simple-benchmark/
go build ./
./dynamodb-simple-benchmark -seqpk=true  -seqsk=true -table=benchmark -con=2 -max=10000 -datasize=100 -pk=US -sk=20200101120001 -region=ap-northeast-1 

```

option
```bash
  -con int
    	Concurrent Request No (default 2)
  -max int
    	max Request count (default 100)
  -pk string
    	partition key string (default "pk")
  -sk string
    	sort key string (default "sk")
  -datasize int
        test data attribute value datasize(random text)  (default 16byte)
  -region string
    	Use aws region (default "ap-northeast-1")
  -seqpk
    	bool:generate sequence pk data(ex:PK_1,PK_2...) or fix string
  -seqsk
    	bool:generate sequence sk data(ex:SK_1,SK_2...) or fix string

  -table string
    	Use DynamoDB table name (default "benchmark")

go run ./main.go -seqpk=true  -seqsk=true -table=benchmark_pro -con=2 -max=1 -pk=US -sk=20200101120001 -region=ap-northeast-1
```
This project is sample project. for dynamodb stress test tool.
This benchmark tool is intended for simple testing.

* Concurrency control
* Simple generation of PartitionKey and SortKey strings
* Set the number of requests

I created this primarily to confirm the mechanism.
https://aws.amazon.com/jp/about-aws/whats-new/2019/11/amazon-dynamodb-adaptive-capacity-now-handles-imbalanced-workloads-better-by-isolating-frequently-accessed-items-automatically/


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

# License
This library is licensed under the MIT-0 License. See the LICENSE file.
