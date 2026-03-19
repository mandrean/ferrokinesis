// ferrokinesis quickstart — AWS SDK for Go (v2) over TLS
//
// Prerequisites:
//   go mod tidy
//   ferrokinesis running with TLS:
//     cargo install ferrokinesis --features tls
//     ferrokinesis generate-cert
//     ferrokinesis --tls-cert cert.pem --tls-key key.pem
//
// Usage:
//   go run quickstart_tls.go
//   KINESIS_ENDPOINT=https://localhost:4567 go run quickstart_tls.go

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func main() {
	endpoint := os.Getenv("KINESIS_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://localhost:4567"
	}
	stream := "go-example-tls"

	client := kinesis.New(kinesis.Options{
		Region:       "us-east-1",
		BaseEndpoint: &endpoint,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	})
	ctx := context.Background()

	// Create a stream
	fmt.Println("==> CreateStream")
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(stream),
		ShardCount: aws.Int32(2),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Wait for ACTIVE
	for i := 0; i < 30; i++ {
		out, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws.String(stream),
		})
		if err == nil && out.StreamDescription.StreamStatus == types.StreamStatusActive {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Put a record
	fmt.Println("==> PutRecord")
	put, err := client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(stream),
		Data:         []byte("hello world"),
		PartitionKey: aws.String("pk1"),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Get records
	fmt.Println("==> GetRecords")
	iterResp, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(stream),
		ShardId:           put.ShardId,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		log.Fatal(err)
	}

	records, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: iterResp.ShardIterator,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, r := range records.Records {
		fmt.Printf("%s: %s\n", *r.PartitionKey, string(r.Data))
	}

	// Clean up
	fmt.Println("==> DeleteStream")
	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(stream),
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Done.")
}
