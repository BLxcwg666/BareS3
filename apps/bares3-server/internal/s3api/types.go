package s3api

import (
	"encoding/xml"

	"bares3-server/internal/storage"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type listBucketsResult struct {
	XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
	Xmlns   string       `xml:"xmlns,attr"`
	Owner   ownerInfo    `xml:"Owner"`
	Buckets bucketsBlock `xml:"Buckets"`
}

type bucketsBlock struct {
	Items []bucketEntry `xml:"Bucket"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ownerInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type listObjectsV2Result struct {
	XMLName               xml.Name            `xml:"ListBucketResult"`
	Xmlns                 string              `xml:"xmlns,attr"`
	Name                  string              `xml:"Name"`
	Prefix                string              `xml:"Prefix"`
	KeyCount              int                 `xml:"KeyCount"`
	MaxKeys               int                 `xml:"MaxKeys"`
	IsTruncated           bool                `xml:"IsTruncated"`
	Contents              []listObjectEntry   `xml:"Contents"`
	CommonPrefixes        []commonPrefixEntry `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string              `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string              `xml:"NextContinuationToken,omitempty"`
	StartAfter            string              `xml:"StartAfter,omitempty"`
	EncodingType          string              `xml:"EncodingType,omitempty"`
	Delimiter             string              `xml:"Delimiter,omitempty"`
}

type listObjectsResult struct {
	XMLName        xml.Name            `xml:"ListBucketResult"`
	Xmlns          string              `xml:"xmlns,attr"`
	Name           string              `xml:"Name"`
	Prefix         string              `xml:"Prefix"`
	Marker         string              `xml:"Marker,omitempty"`
	NextMarker     string              `xml:"NextMarker,omitempty"`
	MaxKeys        int                 `xml:"MaxKeys"`
	Delimiter      string              `xml:"Delimiter,omitempty"`
	IsTruncated    bool                `xml:"IsTruncated"`
	Contents       []listObjectEntry   `xml:"Contents"`
	CommonPrefixes []commonPrefixEntry `xml:"CommonPrefixes,omitempty"`
	EncodingType   string              `xml:"EncodingType,omitempty"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name               `xml:"ListMultipartUploadsResult"`
	Xmlns              string                 `xml:"xmlns,attr"`
	Bucket             string                 `xml:"Bucket"`
	KeyMarker          string                 `xml:"KeyMarker"`
	UploadIDMarker     string                 `xml:"UploadIdMarker"`
	NextKeyMarker      string                 `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string                 `xml:"NextUploadIdMarker,omitempty"`
	Prefix             string                 `xml:"Prefix"`
	Delimiter          string                 `xml:"Delimiter,omitempty"`
	MaxUploads         int                    `xml:"MaxUploads"`
	IsTruncated        bool                   `xml:"IsTruncated"`
	Uploads            []multipartUploadEntry `xml:"Upload,omitempty"`
	CommonPrefixes     []commonPrefixEntry    `xml:"CommonPrefixes,omitempty"`
	EncodingType       string                 `xml:"EncodingType,omitempty"`
}

type multipartUploadEntry struct {
	Key          string    `xml:"Key"`
	UploadID     string    `xml:"UploadId"`
	Initiator    ownerInfo `xml:"Initiator"`
	Owner        ownerInfo `xml:"Owner"`
	StorageClass string    `xml:"StorageClass"`
	Initiated    string    `xml:"Initiated"`
}

type listObjectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixEntry struct {
	Prefix string `xml:"Prefix"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name                   `xml:"CompleteMultipartUpload"`
	Parts   []completeMultipartPartXML `xml:"Part"`
}

type completeMultipartPartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag,omitempty"`
}

type listPartsResult struct {
	XMLName              xml.Name        `xml:"ListPartsResult"`
	Xmlns                string          `xml:"xmlns,attr"`
	Bucket               string          `xml:"Bucket"`
	Key                  string          `xml:"Key"`
	UploadID             string          `xml:"UploadId"`
	PartNumberMarker     int             `xml:"PartNumberMarker"`
	NextPartNumberMarker int             `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int             `xml:"MaxParts"`
	IsTruncated          bool            `xml:"IsTruncated"`
	Parts                []listPartEntry `xml:"Part"`
}

type listPartEntry struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size"`
}

type bucketLocationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Value   string   `xml:",chardata"`
}

type deleteObjectsRequest struct {
	XMLName xml.Name                   `xml:"Delete"`
	Quiet   bool                       `xml:"Quiet"`
	Objects []deleteObjectsRequestItem `xml:"Object"`
}

type deleteObjectsRequestItem struct {
	Key string `xml:"Key"`
}

type deleteObjectsResult struct {
	XMLName xml.Name                 `xml:"DeleteResult"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Deleted []deleteObjectsDeleted   `xml:"Deleted,omitempty"`
	Errors  []deleteObjectsErrorItem `xml:"Error,omitempty"`
}

type deleteObjectsDeleted struct {
	Key string `xml:"Key"`
}

type deleteObjectsErrorItem struct {
	Key     string `xml:"Key,omitempty"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr,omitempty"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag,omitempty"`
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr,omitempty"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag,omitempty"`
}

type listResponseItem struct {
	Kind   string
	Value  string
	Object storage.ObjectInfo
}

type multipartListItem struct {
	Kind   string
	Value  string
	Upload storage.MultipartUploadInfo
}

type s3ErrorSpec struct {
	Status  int
	Code    string
	Message string
}

type copyRangeSpec struct {
	Start  int64
	Length int64
}
