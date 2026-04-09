package storage

import (
	"fmt"
	"strings"
)

const (
	BucketAccessPrivate = "private"
	BucketAccessPublic  = "public"
	BucketAccessCustom  = "custom"

	BucketAccessActionPublic        = "public"
	BucketAccessActionAuthenticated = "authenticated"
	BucketAccessActionDeny          = "deny"
)

func NormalizeBucketAccessMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", BucketAccessPrivate:
		return BucketAccessPrivate
	case BucketAccessPublic:
		return BucketAccessPublic
	case BucketAccessCustom:
		return BucketAccessCustom
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func IsBucketPublicAccess(value string) bool {
	return NormalizeBucketAccessMode(value) == BucketAccessPublic
}

func validateBucketAccessMode(value string) error {
	switch NormalizeBucketAccessMode(value) {
	case BucketAccessPrivate, BucketAccessPublic, BucketAccessCustom:
		return nil
	default:
		return fmt.Errorf("%w: access mode must be private, public, or custom", ErrInvalidBucketAccess)
	}
}

func NormalizeBucketAccessAction(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", BucketAccessActionAuthenticated:
		return BucketAccessActionAuthenticated
	case BucketAccessActionPublic:
		return BucketAccessActionPublic
	case BucketAccessActionDeny:
		return BucketAccessActionDeny
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func normalizeBucketAccessPrefix(value string) string {
	return strings.TrimPrefix(strings.TrimSpace(strings.ReplaceAll(value, "\\", "/")), "/")
}

func validateBucketAccessAction(value string) error {
	switch NormalizeBucketAccessAction(value) {
	case BucketAccessActionPublic, BucketAccessActionAuthenticated, BucketAccessActionDeny:
		return nil
	default:
		return fmt.Errorf("%w: action must be public, authenticated, or deny", ErrInvalidBucketAccess)
	}
}

func PresetBucketAccessPolicy(mode string) BucketAccessPolicy {
	if NormalizeBucketAccessMode(mode) == BucketAccessPublic {
		return BucketAccessPolicy{DefaultAction: BucketAccessActionPublic}
	}
	return BucketAccessPolicy{DefaultAction: BucketAccessActionAuthenticated}
}

func NormalizeBucketAccessPolicy(policy BucketAccessPolicy) BucketAccessPolicy {
	normalized := BucketAccessPolicy{
		DefaultAction: NormalizeBucketAccessAction(policy.DefaultAction),
		Rules:         []BucketAccessRule{},
	}
	if len(policy.Rules) == 0 {
		return normalized
	}

	normalized.Rules = make([]BucketAccessRule, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		normalized.Rules = append(normalized.Rules, BucketAccessRule{
			Prefix: normalizeBucketAccessPrefix(rule.Prefix),
			Action: NormalizeBucketAccessAction(rule.Action),
			Note:   strings.TrimSpace(rule.Note),
		})
	}
	return normalized
}

func validateBucketAccessPolicy(policy BucketAccessPolicy) error {
	if err := validateBucketAccessAction(policy.DefaultAction); err != nil {
		return err
	}
	for index, rule := range policy.Rules {
		if normalizeBucketAccessPrefix(rule.Prefix) == "" {
			return fmt.Errorf("%w: rule %d prefix is required", ErrInvalidBucketAccess, index+1)
		}
		if err := validateBucketAccessAction(rule.Action); err != nil {
			return fmt.Errorf("%w: rule %d %s", ErrInvalidBucketAccess, index+1, strings.TrimPrefix(err.Error(), ErrInvalidBucketAccess.Error()+": "))
		}
	}
	return nil
}

func EffectiveBucketAccessAction(mode string, policy BucketAccessPolicy, key string) string {
	switch NormalizeBucketAccessMode(mode) {
	case BucketAccessPublic:
		return BucketAccessActionPublic
	case BucketAccessPrivate:
		return BucketAccessActionAuthenticated
	case BucketAccessCustom:
		normalized := NormalizeBucketAccessPolicy(policy)
		normalizedKey := normalizeBucketAccessPrefix(key)
		for _, rule := range normalized.Rules {
			if strings.HasPrefix(normalizedKey, rule.Prefix) {
				return rule.Action
			}
		}
		return normalized.DefaultAction
	default:
		return BucketAccessActionAuthenticated
	}
}
