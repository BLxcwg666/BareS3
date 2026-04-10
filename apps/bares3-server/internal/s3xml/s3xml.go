package s3xml

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ErrorOptions struct {
	Code       string
	Message    string
	Resource   string
	RequestID  string
	HostID     string
	Region     string
	BucketName string
}

type errorResponse struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName,omitempty"`
	Resource   string   `xml:"Resource,omitempty"`
	Region     string   `xml:"Region,omitempty"`
	RequestID  string   `xml:"RequestId"`
	HostID     string   `xml:"HostId,omitempty"`
}

func Write(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(payload)
}

func WriteError(w http.ResponseWriter, r *http.Request, status int, options ErrorOptions) {
	requestID := strings.TrimSpace(options.RequestID)
	if requestID == "" {
		requestID = fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	region := strings.TrimSpace(options.Region)
	resource := strings.TrimSpace(options.Resource)
	if resource == "" && r != nil && r.URL != nil {
		resource = r.URL.Path
	}
	hostID := strings.TrimSpace(options.HostID)
	if hostID == "" && r != nil {
		hostID = strings.TrimSpace(r.Host)
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("X-Amz-Request-Id", requestID)
	if region != "" {
		w.Header().Set("X-Amz-Bucket-Region", region)
	}
	w.WriteHeader(status)

	if r != nil && r.Method == http.MethodHead {
		return
	}

	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(errorResponse{
		Code:       options.Code,
		Message:    options.Message,
		BucketName: strings.TrimSpace(options.BucketName),
		Resource:   resource,
		Region:     region,
		RequestID:  requestID,
		HostID:     hostID,
	})
}
