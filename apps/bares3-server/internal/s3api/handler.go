package s3api

import (
	"bares3-server/internal/config"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

func handleObjectRequest(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, credential s3creds.Credential, bucket, key string) {
	if hasQueryValue(r, "uploadId") {
		handleMultipartUploadRequest(w, r, cfg, store, credential, bucket, key)
		return
	}
	if hasQueryValue(r, "uploads") {
		if r.Method != http.MethodPost {
			writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "multipart initiation requires POST")
			return
		}
		handleInitiateMultipartUpload(w, r, store, bucket, key)
		return
	}

	switch r.Method {
	case http.MethodPut:
		if strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source")) != "" {
			handleCopyObject(w, r, store, credential, bucket, key)
			return
		}
		object, err := store.PutObject(r.Context(), storage.PutObjectInput{
			Bucket:             bucket,
			Key:                key,
			Body:               r.Body,
			ContentType:        strings.TrimSpace(r.Header.Get("Content-Type")),
			CacheControl:       strings.TrimSpace(r.Header.Get("Cache-Control")),
			ContentDisposition: strings.TrimSpace(r.Header.Get("Content-Disposition")),
			UserMetadata:       extractUserMetadata(r.Header),
		})
		if err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		if object.ETag != "" {
			w.Header().Set("ETag", `"`+object.ETag+`"`)
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		if !authorizeObjectRead(w, r, store, bucket, key) {
			return
		}
		file, object, err := store.OpenObject(r.Context(), bucket, key)
		if err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		defer func() {
			_ = file.Close()
		}()
		applyObjectHeaders(w, object)
		http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
	case http.MethodDelete:
		if err := store.DeleteObject(r.Context(), bucket, key); err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
			writeStorageAsS3Error(w, r, err)
			return
		}
		if _, err := shareLinks.RemoveByObject(r.Context(), bucket, key); err != nil {
			writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for object requests")
	}
}

func handleGetBucketLocation(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	if _, err := store.GetBucket(r.Context(), bucket); err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}
	writeS3XML(w, http.StatusOK, bucketLocationConstraint{
		Xmlns: s3Namespace,
		Value: w.Header().Get("X-Amz-Bucket-Region"),
	})
}

func handleListMultipartUploads(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxUploads, ok := parseBoundedIntQuery(w, r, "max-uploads", 1000, "max-uploads must be a non-negative integer")
	if !ok {
		return
	}

	delimiter := r.URL.Query().Get("delimiter")
	keyMarker := r.URL.Query().Get("key-marker")
	uploadIDMarker := r.URL.Query().Get("upload-id-marker")
	encodingType := strings.TrimSpace(r.URL.Query().Get("encoding-type"))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "encoding-type must be url")
		return
	}

	uploads, err := store.ListMultipartUploads(r.Context(), bucket)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	result, err := buildListMultipartUploadsResult(bucket, uploads, r.URL.Query().Get("prefix"), delimiter, keyMarker, uploadIDMarker, encodingType, maxUploads)
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	writeS3XML(w, http.StatusOK, result)
}

func handleListObjects(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxKeys, ok := parseMaxKeys(w, r)
	if !ok {
		return
	}

	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")
	encodingType := strings.TrimSpace(r.URL.Query().Get("encoding-type"))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "encoding-type must be url")
		return
	}

	items, err := store.ListObjects(r.Context(), bucket, storage.ListObjectsOptions{
		Prefix: r.URL.Query().Get("prefix"),
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	result, err := buildListObjectsResult(bucket, items, r.URL.Query().Get("prefix"), delimiter, marker, encodingType, maxKeys)
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	writeS3XML(w, http.StatusOK, result)
}

func handleListObjectsV2(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxKeys, ok := parseMaxKeys(w, r)
	if !ok {
		return
	}

	delimiter := r.URL.Query().Get("delimiter")
	continuationToken := r.URL.Query().Get("continuation-token")
	startAfter := r.URL.Query().Get("start-after")
	encodingType := strings.TrimSpace(r.URL.Query().Get("encoding-type"))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "encoding-type must be url")
		return
	}

	items, err := store.ListObjects(r.Context(), bucket, storage.ListObjectsOptions{
		Prefix: r.URL.Query().Get("prefix"),
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	result, err := buildListObjectsV2Result(bucket, items, r.URL.Query().Get("prefix"), delimiter, continuationToken, startAfter, encodingType, maxKeys)
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	writeS3XML(w, http.StatusOK, result)
}

func handleCopyObject(w http.ResponseWriter, r *http.Request, store *storage.Store, credential s3creds.Credential, bucket, key string) {
	sourceBucket, sourceKey, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	if !authorizeBucketRequest(w, r, credential, sourceBucket, false) {
		return
	}
	if !authorizeObjectRead(w, r, store, sourceBucket, sourceKey) {
		return
	}

	sourceFile, sourceObject, err := store.OpenObject(r.Context(), sourceBucket, sourceKey)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	if spec := evaluateCopySourcePreconditions(r, sourceObject); spec != nil {
		writeS3Error(w, r, spec.Status, spec.Code, spec.Message)
		return
	}

	metadataDirective := strings.ToUpper(strings.TrimSpace(r.Header.Get("X-Amz-Metadata-Directive")))
	if metadataDirective == "" {
		metadataDirective = "COPY"
	}
	if metadataDirective != "COPY" && metadataDirective != "REPLACE" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "x-amz-metadata-directive must be COPY or REPLACE")
		return
	}

	putInput := storage.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   sourceFile,
	}
	if metadataDirective == "COPY" {
		putInput.ContentType = sourceObject.ContentType
		putInput.CacheControl = sourceObject.CacheControl
		putInput.ContentDisposition = sourceObject.ContentDisposition
		putInput.UserMetadata = cloneMetadataMap(sourceObject.UserMetadata)
	} else {
		putInput.ContentType = firstNonEmpty(strings.TrimSpace(r.Header.Get("Content-Type")), sourceObject.ContentType)
		putInput.CacheControl = firstNonEmpty(strings.TrimSpace(r.Header.Get("Cache-Control")), sourceObject.CacheControl)
		putInput.ContentDisposition = firstNonEmpty(strings.TrimSpace(r.Header.Get("Content-Disposition")), sourceObject.ContentDisposition)
		putInput.UserMetadata = mergeMetadataDirective(sourceObject.UserMetadata, extractUserMetadata(r.Header))
	}

	object, err := store.PutObject(r.Context(), putInput)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	writeS3XML(w, http.StatusOK, copyObjectResult{
		Xmlns:        s3Namespace,
		LastModified: formatS3Time(object.LastModified),
		ETag:         `"` + object.ETag + `"`,
	})
}

func handleDeleteObjects(w http.ResponseWriter, r *http.Request, store *storage.Store, shareLinks *sharelink.Store, bucket string) {
	if _, err := store.GetBucket(r.Context(), bucket); err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	requestPayload := deleteObjectsRequest{}
	if err := xml.NewDecoder(r.Body).Decode(&requestPayload); err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "MalformedXML", "delete objects body is invalid")
		return
	}

	result := deleteObjectsResult{Xmlns: s3Namespace}
	for _, item := range requestPayload.Objects {
		key := strings.TrimSpace(item.Key)
		if key == "" {
			result.Errors = append(result.Errors, deleteObjectsErrorItem{
				Code:    "InvalidArgument",
				Message: "object key is required",
			})
			continue
		}

		err := store.DeleteObject(r.Context(), bucket, key)
		switch {
		case err == nil:
		case errors.Is(err, storage.ErrObjectNotFound):
		case errors.Is(err, storage.ErrInvalidObjectKey):
			result.Errors = append(result.Errors, deleteObjectsErrorItem{
				Key:     key,
				Code:    "InvalidArgument",
				Message: err.Error(),
			})
			continue
		default:
			writeStorageAsS3Error(w, r, err)
			return
		}

		if _, err := shareLinks.RemoveByObject(r.Context(), bucket, key); err != nil {
			writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		if !requestPayload.Quiet {
			result.Deleted = append(result.Deleted, deleteObjectsDeleted{Key: key})
		}
	}

	writeS3XML(w, http.StatusOK, result)
}

func handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key string) {
	upload, err := store.InitiateMultipartUpload(r.Context(), storage.InitiateMultipartUploadInput{
		Bucket:             bucket,
		Key:                key,
		ContentType:        strings.TrimSpace(r.Header.Get("Content-Type")),
		CacheControl:       strings.TrimSpace(r.Header.Get("Cache-Control")),
		ContentDisposition: strings.TrimSpace(r.Header.Get("Content-Disposition")),
		UserMetadata:       extractUserMetadata(r.Header),
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	writeS3XML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns:    s3Namespace,
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadID: upload.UploadID,
	})
}

func handleMultipartUploadRequest(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, credential s3creds.Credential, bucket, key string) {
	uploadID := strings.TrimSpace(r.URL.Query().Get("uploadId"))
	if uploadID == "" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "uploadId is required")
		return
	}

	switch r.Method {
	case http.MethodPut:
		rawPartNumber := strings.TrimSpace(r.URL.Query().Get("partNumber"))
		partNumber, err := strconv.Atoi(rawPartNumber)
		if err != nil {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "partNumber must be a positive integer")
			return
		}
		if strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source")) != "" {
			handleUploadPartCopy(w, r, store, credential, bucket, key, uploadID, partNumber)
			return
		}
		part, err := store.UploadPart(r.Context(), storage.UploadPartInput{
			Bucket:     bucket,
			Key:        key,
			UploadID:   uploadID,
			PartNumber: partNumber,
			Body:       r.Body,
		})
		if err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.Header().Set("ETag", `"`+part.ETag+`"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		handleListParts(w, r, store, bucket, key, uploadID)
	case http.MethodPost:
		handleCompleteMultipartUpload(w, r, cfg, store, bucket, key, uploadID)
	case http.MethodDelete:
		if err := store.AbortMultipartUpload(r.Context(), bucket, key, uploadID); err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for multipart requests")
	}
}

func handleUploadPartCopy(w http.ResponseWriter, r *http.Request, store *storage.Store, credential s3creds.Credential, bucket, key, uploadID string, partNumber int) {
	sourceBucket, sourceKey, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	if !authorizeBucketRequest(w, r, credential, sourceBucket, false) {
		return
	}
	if !authorizeObjectRead(w, r, store, sourceBucket, sourceKey) {
		return
	}

	sourceFile, sourceObject, err := store.OpenObject(r.Context(), sourceBucket, sourceKey)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	if spec := evaluateCopySourcePreconditions(r, sourceObject); spec != nil {
		writeS3Error(w, r, spec.Status, spec.Code, spec.Message)
		return
	}

	body := io.Reader(sourceFile)
	if rawRange := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-Range")); rawRange != "" {
		rangeSpec, spec := parseCopySourceRange(rawRange, sourceObject.Size)
		if spec != nil {
			writeS3Error(w, r, spec.Status, spec.Code, spec.Message)
			return
		}
		body = io.NewSectionReader(sourceFile, rangeSpec.Start, rangeSpec.Length)
	}

	part, err := store.UploadPart(r.Context(), storage.UploadPartInput{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: partNumber,
		Body:       body,
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	writeS3XML(w, http.StatusOK, copyPartResult{
		Xmlns:        s3Namespace,
		LastModified: formatS3Time(part.LastModified),
		ETag:         `"` + part.ETag + `"`,
	})
}

func handleListParts(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key, uploadID string) {
	maxParts := 1000
	if raw := strings.TrimSpace(r.URL.Query().Get("max-parts")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "max-parts must be a non-negative integer")
			return
		}
		maxParts = parsed
	}
	if maxParts > 1000 {
		maxParts = 1000
	}

	partNumberMarker := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("part-number-marker")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "part-number-marker must be a non-negative integer")
			return
		}
		partNumberMarker = parsed
	}

	parts, err := store.ListParts(r.Context(), bucket, key, uploadID)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	visible := make([]storage.MultipartPartInfo, 0, len(parts))
	for _, part := range parts {
		if part.PartNumber > partNumberMarker {
			visible = append(visible, part)
		}
	}

	isTruncated := maxParts >= 0 && len(visible) > maxParts
	if isTruncated {
		visible = visible[:maxParts]
	}

	entries := make([]listPartEntry, 0, len(visible))
	for _, part := range visible {
		entries = append(entries, listPartEntry{
			PartNumber:   part.PartNumber,
			LastModified: formatS3Time(part.LastModified),
			ETag:         `"` + part.ETag + `"`,
			Size:         part.Size,
		})
	}

	nextMarker := 0
	if isTruncated && len(visible) > 0 {
		nextMarker = visible[len(visible)-1].PartNumber
	}

	writeS3XML(w, http.StatusOK, listPartsResult{
		Xmlns:                s3Namespace,
		Bucket:               bucket,
		Key:                  key,
		UploadID:             uploadID,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                entries,
	})
}

func handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, bucket, key, uploadID string) {
	requestPayload := completeMultipartUploadRequest{}
	if err := xml.NewDecoder(r.Body).Decode(&requestPayload); err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "MalformedXML", "complete multipart upload body is invalid")
		return
	}

	parts := make([]storage.CompletedPart, 0, len(requestPayload.Parts))
	for _, part := range requestPayload.Parts {
		parts = append(parts, storage.CompletedPart{PartNumber: part.PartNumber, ETag: part.ETag})
	}

	object, err := store.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, parts)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	runtimeSettings, err := store.RuntimeSettings(r.Context())
	if err != nil {
		writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	location := runtimeSettings.S3BaseURL
	if parsed, err := url.Parse(runtimeSettings.S3BaseURL); err == nil {
		parsed.Path = joinURLPath(parsed.Path, bucket, key)
		location = parsed.String()
	}

	writeS3XML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns:    s3Namespace,
		Location: location,
		Bucket:   bucket,
		Key:      key,
		ETag:     `"` + object.ETag + `"`,
	})
}
