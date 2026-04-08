package storage

import (
	"fmt"
	"strings"
)

const (
	BucketAccessPrivate = "private"
	BucketAccessPublic  = "public"
)

func NormalizeBucketAccessMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", BucketAccessPrivate:
		return BucketAccessPrivate
	case BucketAccessPublic:
		return BucketAccessPublic
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func IsBucketPublicAccess(value string) bool {
	return NormalizeBucketAccessMode(value) == BucketAccessPublic
}

func validateBucketAccessMode(value string) error {
	switch NormalizeBucketAccessMode(value) {
	case BucketAccessPrivate, BucketAccessPublic:
		return nil
	default:
		return fmt.Errorf("%w: access mode must be private or public", ErrInvalidBucketAccess)
	}
}
