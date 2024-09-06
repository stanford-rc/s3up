package main

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/transport/http"
)

// mergeObjectAttributesHeader manipulates the request client-side before it is
// dispatched, and merges multiple X-Amz-Object-Attributes headers into one
// comma-separated line, working around the bug in MinIO logged in
// https://github.com/minio/minio/issues/20267.
func mergeObjectAttributesHeader(opt *s3.Options) {
	opt.APIOptions = append(opt.APIOptions, func(stack *middleware.Stack) error {
		return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
			"mergeObjectAttributesHeaders",
			func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
				out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
			) {
				const headerKey = `X-Amz-Object-Attributes`
				req := in.Request.(*http.Request)
				values := req.Header.Values(headerKey)
				req.Header.Set(headerKey, strings.Join(values, ","))

				in.Request = req
				return next.HandleFinalize(ctx, in)
			},
		), middleware.Before)
	})
}
