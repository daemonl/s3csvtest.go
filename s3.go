package s3csvtest

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Mock struct {
	s3iface.S3API
	objects map[string][]byte
}

func NewS3Mock() *S3Mock {
	return &S3Mock{
		objects: map[string][]byte{},
	}
}

func (f *S3Mock) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	gotBytes, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	f.objects[fmt.Sprintf("s3://%s/%s", aws.StringValue(input.Bucket), aws.StringValue(input.Key))] = gotBytes
	return &s3.PutObjectOutput{}, nil
}

func (f *S3Mock) AssertCSV(t testing.TB, bucket, key string, assertions []CSVAssertion) {
	t.Helper()

	obj, ok := f.objects[fmt.Sprintf("s3://%s/%s", bucket, key)]
	if !ok {
		t.Fatalf("no object uploaded to bucket %s key %s", bucket, key)
	}

	AssertCSV(t, obj, assertions)
}
