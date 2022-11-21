package belastic

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/grpc-boot/base"
)

type Response struct {
	Status int    `json:"status"`
	Body   []byte `json:"body"`
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

func (r *Response) UnmarshalBulkResult() (*BulkResult, error) {
	br := &BulkResult{}
	err := base.JsonUnmarshal(r.Body, br)
	if err != nil {
		return nil, err
	}

	return br, nil
}

func (r *Response) UnmarshalSearchResult() (*SearchResult, error) {
	sr := &SearchResult{}
	err := base.JsonUnmarshal(r.Body, sr)
	if err != nil {
		return nil, err
	}

	return sr, nil
}
