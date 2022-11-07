package s3csvtest

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type S3Mock struct {
	s3iface.S3API
	objects map[string][]byte
	t       testing.TB
}

func NewS3Mock(t testing.TB) *S3Mock {
	return &S3Mock{
		objects: map[string][]byte{},
		t:       t,
	}
}

func (f *S3Mock) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	key := fmt.Sprintf("s3://%s/%s", aws.StringValue(input.Bucket), aws.StringValue(input.Key))
	f.t.Logf("S3Mock Put: %s", key)
	gotBytes, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	f.objects[key] = gotBytes
	return &s3.PutObjectOutput{}, nil
}

func (f *S3Mock) GetObjectBytes(t testing.TB, bucket, key string) []byte {
	t.Helper()
	obj, ok := f.objects[fmt.Sprintf("s3://%s/%s", bucket, key)]
	if !ok {
		t.Fatalf("no object uploaded to bucket %s key %s", bucket, key)
	}
	return obj
}

func (f *S3Mock) AssertCSV(t testing.TB, bucket, key string, assertions []CSVAssertion) {
	t.Helper()
	obj := f.GetObjectBytes(t, bucket, key)
	AssertCSV(t, obj, assertions)
}

func (f *S3Mock) GetObjectRequest(input *s3.GetObjectInput) (*request.Request, *s3.GetObjectOutput) {

	return &request.Request{

		Operation: &request.Operation{
			Name:       "TEST",
			HTTPMethod: "GET",
		},
		HTTPRequest: &http.Request{
			Host: "127.0.0.0",
			URL: &url.URL{
				Path: aws.StringValue(input.Key),
			},
		},
	}, nil
}
