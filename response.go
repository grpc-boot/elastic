package elastic

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/grpc-boot/elastic/results"

	"github.com/grpc-boot/base"
)

type Response struct {
	Status int    `json:"status"`
	Body   []byte `json:"body"`
}

func (r *Response) Is(status int) bool {
	return r.Status == status
}

func (r *Response) IsOk() bool {
	return r.Status == http.StatusOK || r.Status == http.StatusCreated
}

func (r *Response) Error() (err error) {
	if r.IsOk() {
		return nil
	}

	errMsg := strings.Builder{}
	status := strconv.Itoa(r.Status)

	n := 7 + len(status) + 11 + len(r.Body)

	errMsg.Grow(n)

	errMsg.WriteString(`status:`)
	errMsg.WriteString(status)
	errMsg.WriteString(` error msg:`)
	errMsg.Write(r.Body)

	return errors.New(errMsg.String())
}

func (r *Response) UnmarshalBulkResult() (*results.BulkResult, error) {
	br := &results.BulkResult{}
	err := base.JsonUnmarshal(r.Body, br)
	if err != nil {
		return nil, err
	}

	return br, nil
}

func (r *Response) UnmarshalSearchResult() (*results.SearchResult, error) {
	sr := &results.SearchResult{}
	err := base.JsonUnmarshal(r.Body, sr)
	if err != nil {
		return nil, err
	}

	return sr, nil
}

func (r *Response) UnmarshalIndexResult() (*results.IndexResult, error) {
	ir := &results.IndexResult{}
	err := base.JsonUnmarshal(r.Body, ir)
	if err != nil {
		return nil, err
	}

	return ir, nil
}

func (r *Response) UnmarshalDocumentsResult() (*results.DocumentsResult, error) {
	dr := &results.DocumentsResult{}
	err := base.JsonUnmarshal(r.Body, dr)
	if err != nil {
		return nil, err
	}

	return dr, nil
}

func (r *Response) UnmarshalDocumentResult() (*results.DocumentResult, error) {
	dr := &results.DocumentResult{}
	err := base.JsonUnmarshal(r.Body, dr)
	if err != nil {
		return nil, err
	}

	return dr, nil
}

func (r *Response) UnmarshalSqlSearchResult() (*results.SqlSearchResult, error) {
	ssr := &results.SqlSearchResult{}
	err := base.JsonUnmarshal(r.Body, ssr)
	if err != nil {
		return nil, err
	}

	return ssr, nil
}
