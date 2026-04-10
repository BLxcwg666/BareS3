package s3api

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/storage"
)

func buildListMultipartUploadsResult(bucket string, uploads []storage.MultipartUploadInfo, prefix, delimiter, keyMarker, uploadIDMarker, encodingType string, maxUploads int) (listMultipartUploadsResult, error) {
	items := buildMultipartUploadItems(uploads, prefix, delimiter)
	startIndex := findMultipartStartIndex(items, keyMarker, uploadIDMarker)
	remaining := items[startIndex:]
	emitted := remaining
	isTruncated := false
	if maxUploads >= 0 && len(emitted) > maxUploads {
		emitted = emitted[:maxUploads]
		isTruncated = true
	}

	result := listMultipartUploadsResult{
		Xmlns:          s3Namespace,
		Bucket:         bucket,
		KeyMarker:      encodeListValue(keyMarker, encodingType),
		UploadIDMarker: uploadIDMarker,
		Prefix:         encodeListValue(prefix, encodingType),
		Delimiter:      encodeListValue(delimiter, encodingType),
		MaxUploads:     maxUploads,
		IsTruncated:    isTruncated,
		EncodingType:   encodingType,
	}

	for _, item := range emitted {
		switch item.Kind {
		case "prefix":
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixEntry{Prefix: encodeListValue(item.Value, encodingType)})
		case "upload":
			entry := multipartUploadEntry{
				Key:          encodeListValue(item.Upload.Key, encodingType),
				UploadID:     item.Upload.UploadID,
				Initiator:    ownerInfo{ID: "bares3", DisplayName: "BareS3"},
				Owner:        ownerInfo{ID: "bares3", DisplayName: "BareS3"},
				StorageClass: "STANDARD",
				Initiated:    formatS3Time(item.Upload.CreatedAt),
			}
			result.Uploads = append(result.Uploads, entry)
		}
	}

	if isTruncated && len(emitted) > 0 {
		last := emitted[len(emitted)-1]
		result.NextKeyMarker = encodeListValue(last.Value, encodingType)
		if last.Kind == "upload" {
			result.NextUploadIDMarker = last.Upload.UploadID
		}
	}

	return result, nil
}

func findMultipartStartIndex(items []multipartListItem, keyMarker, uploadIDMarker string) int {
	if strings.TrimSpace(keyMarker) == "" {
		return 0
	}
	startIndex := 0
	for startIndex < len(items) {
		item := items[startIndex]
		if item.Value < keyMarker {
			startIndex++
			continue
		}
		if item.Value > keyMarker {
			break
		}
		if item.Kind == "prefix" {
			startIndex++
			continue
		}
		if strings.TrimSpace(uploadIDMarker) == "" {
			startIndex++
			continue
		}
		startIndex++
		if item.Upload.UploadID == uploadIDMarker {
			return startIndex
		}
	}
	return startIndex
}

func buildMultipartUploadItems(uploads []storage.MultipartUploadInfo, prefix, delimiter string) []multipartListItem {
	items := make([]multipartListItem, 0, len(uploads))
	seenPrefixes := make(map[string]struct{})
	for _, upload := range uploads {
		if prefix != "" && !strings.HasPrefix(upload.Key, prefix) {
			continue
		}
		if delimiter != "" {
			remainder := strings.TrimPrefix(upload.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				commonPrefix := prefix + remainder[:index+len(delimiter)]
				if _, ok := seenPrefixes[commonPrefix]; !ok {
					seenPrefixes[commonPrefix] = struct{}{}
					items = append(items, multipartListItem{Kind: "prefix", Value: commonPrefix})
				}
				continue
			}
		}
		items = append(items, multipartListItem{Kind: "upload", Value: upload.Key, Upload: upload})
	}

	sort.Slice(items, func(i, j int) bool {
		return compareMultipartListItems(items[i], items[j]) < 0
	})
	return items
}

func compareMultipartListItems(left, right multipartListItem) int {
	if left.Value != right.Value {
		return strings.Compare(left.Value, right.Value)
	}
	if left.Kind != right.Kind {
		return strings.Compare(left.Kind, right.Kind)
	}
	if left.Kind != "upload" {
		return 0
	}
	if !left.Upload.CreatedAt.Equal(right.Upload.CreatedAt) {
		if left.Upload.CreatedAt.Before(right.Upload.CreatedAt) {
			return -1
		}
		return 1
	}
	return strings.Compare(left.Upload.UploadID, right.Upload.UploadID)
}

func buildListObjectsResult(bucket string, objects []storage.ObjectInfo, prefix, delimiter, marker, encodingType string, maxKeys int) (listObjectsResult, error) {
	entries, err := buildListItems(objects, prefix, delimiter)
	if err != nil {
		return listObjectsResult{}, err
	}

	startIndex := 0
	if marker != "" {
		for startIndex < len(entries) && entries[startIndex].Value <= marker {
			startIndex++
		}
	}

	remaining := entries[startIndex:]
	emitted := remaining
	isTruncated := false
	if maxKeys >= 0 && len(emitted) > maxKeys {
		emitted = emitted[:maxKeys]
		isTruncated = true
	}

	result := listObjectsResult{
		Xmlns:        s3Namespace,
		Name:         bucket,
		Prefix:       encodeListValue(prefix, encodingType),
		Marker:       encodeListValue(marker, encodingType),
		MaxKeys:      maxKeys,
		Delimiter:    encodeListValue(delimiter, encodingType),
		IsTruncated:  isTruncated,
		EncodingType: encodingType,
	}

	for _, entry := range emitted {
		switch entry.Kind {
		case "prefix":
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixEntry{Prefix: encodeListValue(entry.Value, encodingType)})
		case "content":
			item := listObjectEntry{
				Key:          encodeListValue(entry.Object.Key, encodingType),
				LastModified: formatS3Time(entry.Object.LastModified),
				Size:         entry.Object.Size,
				StorageClass: "STANDARD",
			}
			if entry.Object.ETag != "" {
				item.ETag = `"` + entry.Object.ETag + `"`
			}
			result.Contents = append(result.Contents, item)
		}
	}

	if isTruncated && delimiter != "" && len(emitted) > 0 {
		result.NextMarker = encodeListValue(emitted[len(emitted)-1].Value, encodingType)
	}

	return result, nil
}

func buildListItems(objects []storage.ObjectInfo, prefix, delimiter string) ([]listResponseItem, error) {
	items := make([]listResponseItem, 0, len(objects))
	seenPrefixes := make(map[string]struct{})
	for _, object := range objects {
		if prefix != "" && !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		if delimiter != "" {
			remainder := strings.TrimPrefix(object.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				commonPrefix := prefix + remainder[:index+len(delimiter)]
				if _, ok := seenPrefixes[commonPrefix]; !ok {
					seenPrefixes[commonPrefix] = struct{}{}
					items = append(items, listResponseItem{Kind: "prefix", Value: commonPrefix})
				}
				continue
			}
		}
		items = append(items, listResponseItem{Kind: "content", Value: object.Key, Object: object})
	}

	sort.Slice(items, func(i, j int) bool {
		return compareListItems(items[i], items[j]) < 0
	})
	return items, nil
}

func compareListItems(left, right listResponseItem) int {
	if left.Value < right.Value {
		return -1
	}
	if left.Value > right.Value {
		return 1
	}
	if left.Kind < right.Kind {
		return -1
	}
	if left.Kind > right.Kind {
		return 1
	}
	return 0
}

func buildListObjectsV2Result(bucket string, objects []storage.ObjectInfo, prefix, delimiter, continuationToken, startAfter, encodingType string, maxKeys int) (listObjectsV2Result, error) {
	entries, err := buildListItems(objects, prefix, delimiter)
	if err != nil {
		return listObjectsV2Result{}, err
	}

	startIndex := 0
	if continuationToken != "" {
		marker, err := decodeContinuationToken(continuationToken)
		if err != nil {
			return listObjectsV2Result{}, err
		}
		for startIndex < len(entries) && compareListItems(entries[startIndex], marker) <= 0 {
			startIndex++
		}
	} else if startAfter != "" {
		for startIndex < len(entries) && entries[startIndex].Value <= startAfter {
			startIndex++
		}
	}

	remaining := entries[startIndex:]
	emitted := remaining
	isTruncated := false
	if maxKeys >= 0 && len(emitted) > maxKeys {
		emitted = emitted[:maxKeys]
		isTruncated = true
	}

	result := listObjectsV2Result{
		Xmlns:             s3Namespace,
		Name:              bucket,
		Prefix:            encodeListValue(prefix, encodingType),
		MaxKeys:           maxKeys,
		IsTruncated:       isTruncated,
		ContinuationToken: continuationToken,
		StartAfter:        encodeListValue(startAfter, encodingType),
		EncodingType:      encodingType,
		Delimiter:         encodeListValue(delimiter, encodingType),
	}

	for _, entry := range emitted {
		switch entry.Kind {
		case "prefix":
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixEntry{Prefix: encodeListValue(entry.Value, encodingType)})
		case "content":
			item := listObjectEntry{
				Key:          encodeListValue(entry.Object.Key, encodingType),
				LastModified: formatS3Time(entry.Object.LastModified),
				Size:         entry.Object.Size,
				StorageClass: "STANDARD",
			}
			if entry.Object.ETag != "" {
				item.ETag = `"` + entry.Object.ETag + `"`
			}
			result.Contents = append(result.Contents, item)
		}
	}

	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)
	if isTruncated && len(emitted) > 0 {
		result.NextContinuationToken = encodeContinuationToken(emitted[len(emitted)-1])
	}

	return result, nil
}

func encodeContinuationToken(item listResponseItem) string {
	return base64.RawURLEncoding.EncodeToString([]byte(item.Kind + ":" + item.Value))
}

func decodeContinuationToken(value string) (listResponseItem, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return listResponseItem{}, fmt.Errorf("continuation token is invalid")
	}
	kind, marker, ok := strings.Cut(string(decoded), ":")
	if !ok || (kind != "content" && kind != "prefix") {
		return listResponseItem{}, fmt.Errorf("continuation token is invalid")
	}
	return listResponseItem{Kind: kind, Value: marker}, nil
}

func authorizeObjectRead(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key string) bool {
	action, err := store.ResolveBucketObjectAccess(r.Context(), bucket, key)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return false
	}
	if action == storage.BucketAccessActionDeny {
		writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "object access denied by bucket policy")
		return false
	}
	return true
}

func applyObjectHeaders(w http.ResponseWriter, object storage.ObjectInfo) {
	if object.ContentType != "" {
		w.Header().Set("Content-Type", object.ContentType)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}
	for key, value := range object.UserMetadata {
		w.Header().Set("X-Amz-Meta-"+key, value)
	}
}

func extractUserMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for key, values := range header {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-amz-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func parseCopySource(value string) (bucket string, key string, err error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", "", fmt.Errorf("x-amz-copy-source is required")
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	pathValue, rawQuery, _ := strings.Cut(trimmed, "?")
	if rawQuery != "" {
		return "", "", fmt.Errorf("copy source subresources are not supported")
	}
	encodedBucket, encodedKey, ok := strings.Cut(pathValue, "/")
	if !ok || strings.TrimSpace(encodedBucket) == "" || strings.TrimSpace(encodedKey) == "" {
		return "", "", fmt.Errorf("x-amz-copy-source must be bucket/key")
	}
	bucket, err = url.PathUnescape(encodedBucket)
	if err != nil {
		return "", "", fmt.Errorf("x-amz-copy-source bucket is invalid")
	}
	key, err = url.PathUnescape(encodedKey)
	if err != nil {
		return "", "", fmt.Errorf("x-amz-copy-source key is invalid")
	}
	return bucket, key, nil
}

func evaluateCopySourcePreconditions(r *http.Request, object storage.ObjectInfo) *s3ErrorSpec {
	if matchValue := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-Match")); matchValue != "" {
		if !etagListMatches(matchValue, object.ETag) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	} else if raw := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-Unmodified-Since")); raw != "" {
		timestamp, err := parseHTTPTimeHeader(raw)
		if err != nil {
			return &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: err.Error()}
		}
		if isModifiedSince(object.LastModified, timestamp) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	}

	if noneMatchValue := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-None-Match")); noneMatchValue != "" {
		if etagListMatches(noneMatchValue, object.ETag) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	} else if raw := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-Modified-Since")); raw != "" {
		timestamp, err := parseHTTPTimeHeader(raw)
		if err != nil {
			return &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: err.Error()}
		}
		if !isModifiedSince(object.LastModified, timestamp) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	}

	return nil
}

func parseCopySourceRange(value string, size int64) (copyRangeSpec, *s3ErrorSpec) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "bytes=") {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	startRaw, endRaw, ok := strings.Cut(strings.TrimPrefix(trimmed, "bytes="), "-")
	if !ok || strings.TrimSpace(startRaw) == "" || strings.TrimSpace(endRaw) == "" {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startRaw), 10, 64)
	if err != nil || start < 0 {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	end, err := strconv.ParseInt(strings.TrimSpace(endRaw), 10, 64)
	if err != nil || end < start {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	if size <= 0 || start >= size || end >= size {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range is outside the source object"}
	}
	return copyRangeSpec{Start: start, Length: end - start + 1}, nil
}

func etagListMatches(headerValue, etag string) bool {
	normalizedETag := normalizeETag(etag)
	for _, candidate := range strings.Split(headerValue, ",") {
		normalizedCandidate := normalizeETag(candidate)
		if normalizedCandidate == "" {
			continue
		}
		if normalizedCandidate == "*" || normalizedCandidate == normalizedETag {
			return true
		}
	}
	return false
}

func normalizeETag(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "W/")
	trimmed = strings.TrimPrefix(trimmed, "w/")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "*" {
		return trimmed
	}
	return strings.Trim(trimmed, `"`)
}

func encodeListValue(value, encodingType string) string {
	if value == "" || encodingType != "url" {
		return value
	}
	return url.PathEscape(value)
}

func parseHTTPTimeHeader(value string) (time.Time, error) {
	parsed, err := http.ParseTime(strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("time header is invalid")
	}
	return parsed.UTC(), nil
}

func parseBoundedIntQuery(w http.ResponseWriter, r *http.Request, name string, upperBound int, invalidMessage string) (int, bool) {
	value := upperBound
	if raw := strings.TrimSpace(r.URL.Query().Get(name)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", invalidMessage)
			return 0, false
		}
		value = parsed
	}
	if value > upperBound {
		value = upperBound
	}
	return value, true
}

func isModifiedSince(modifiedAt, timestamp time.Time) bool {
	if modifiedAt.IsZero() {
		return false
	}
	return modifiedAt.UTC().Truncate(time.Second).After(timestamp.UTC().Truncate(time.Second))
}

func cloneMetadataMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func mergeMetadataDirective(source, replacement map[string]string) map[string]string {
	_ = source
	if len(replacement) == 0 {
		return nil
	}
	return cloneMetadataMap(replacement)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
