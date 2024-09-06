package main

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3ClientPool implements a simple unbounded cache for reusing *s3.Client.
type S3ClientPool struct {
	shared *s3.Client
	pool   *sync.Pool
}

// NewS3ClientPool initializes a new S3ClientPool which will return *s3.Client
// initialized with the specified configuration and options.  If share is true
// then the S3ClientPool will always return the same *s3.Client, otherwise it
// will allocate new *s3.Client as needed to fulfil Get requests.
//
// Normally an *s3.Client will open multiple socket connections to AWS S3,
// making efficent use of multiple write operations.  In that situation setting
// share to true would make sense.
//
// When talking to MinIO servers it has been observered that an *s3.Client may
// only open a single socket, and that performance improves if we use multiple
// clients.  In this situation set share to false.
func NewS3ClientPool(share bool, cfg aws.Config, opts ...func(*s3.Options)) *S3ClientPool {

	var s3client *s3.Client
	if share {
		s3client = s3.NewFromConfig(cfg, opts...)
	}

	return &S3ClientPool{
		shared: s3client,
		pool: &sync.Pool{
			New: func() any {
				return s3.NewFromConfig(cfg, opts...)
			},
		},
	}
}

// Get returns an *s3.Client. The client must be returned via Put when the
// caller has finished with it.
func (p *S3ClientPool) Get() *s3.Client {
	if p.shared != nil {
		return p.shared
	}
	return p.pool.Get().(*s3.Client)
}

// Put returns an *s3.Client to be added back to the cache pool to become
// available for the next call to Get.
func (p *S3ClientPool) Put(s3client *s3.Client) {
	if p.shared == nil {
		p.pool.Put(s3client)
	}
}
