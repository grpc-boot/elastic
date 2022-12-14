package elastic

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-boot/elastic/results"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
)

type Connection struct {
	client   *http.Client
	baseUrl  string
	username string
	password string
}

func New(opt Option) *Connection {
	option := loadOption(opt)

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(option.DialTimeoutSecond) * time.Second,
			KeepAlive: time.Duration(option.KeepaliveSecond) * time.Second,
		}).DialContext,
		MaxIdleConns:        option.MaxIdleConns,
		MaxIdleConnsPerHost: option.MaxIdleConnsPerHost,
		MaxConnsPerHost:     option.MaxConnsPerHost,
		IdleConnTimeout:     time.Duration(option.IdleConnTimeoutSecond) * time.Second,
	}

	return &Connection{
		client:   &http.Client{Transport: transport},
		baseUrl:  strings.TrimSuffix(option.BaseUrl, "/"),
		username: option.UserName,
		password: option.Password,
	}
}

func (c *Connection) needAuth() bool {
	return c.username != ""
}

func (c *Connection) request(timeout time.Duration, method, path string, params string) (response *Response, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	buffer := bytes.NewBufferString(params)
	req, err := http.NewRequestWithContext(ctx, method, c.baseUrl+path, buffer)
	if err != nil {
		return nil, err
	}

	if c.needAuth() {
		req.SetBasicAuth(c.username, c.password)
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		base.Error("es request failed",
			zaplogger.Method(method),
			zaplogger.Path(path),
			zaplogger.Params(params),
			zaplogger.Error(err),
		)
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		base.Error("request elastic failed",
			zaplogger.Method(method),
			zaplogger.Path(path),
			zaplogger.Params(params),
			zaplogger.Error(err),
		)
		return nil, err
	}

	response = &Response{
		Status: resp.StatusCode,
		Body:   body,
	}

	return
}

func (c *Connection) Put(timeout time.Duration, path string, params string) (*Response, error) {
	return c.request(timeout, http.MethodPut, path, params)
}

func (c *Connection) Post(timeout time.Duration, path string, params string) (*Response, error) {
	return c.request(timeout, http.MethodPost, path, params)
}

func (c *Connection) Get(timeout time.Duration, path string, params string) (*Response, error) {
	return c.request(timeout, http.MethodGet, path, params)
}

func (c *Connection) Delete(timeout time.Duration, path string, params string) (*Response, error) {
	return c.request(timeout, http.MethodDelete, path, params)
}

func (c *Connection) IndexCreate(timeout time.Duration, index string, settings *Settings, mappings *Mappings) (ok bool, err error) {
	var body strings.Builder

	body.WriteString(`{"settings":`)
	body.Write(settings.Marshal())

	if mappings != nil {
		body.WriteString(`,"mappings":`)
		body.Write(mappings.Marshal())
	}

	body.WriteByte('}')

	resp, err := c.request(timeout, http.MethodPut, "/"+index, body.String())
	if err != nil {
		return
	}

	if !resp.IsOk() {
		return false, resp.Error()
	}
	return true, nil
}

func (c *Connection) IndexDelete(timeout time.Duration, index string) (ok bool, err error) {
	resp, err := c.Delete(timeout, "/"+index, "")

	if err != nil {
		return
	}

	if !resp.IsOk() {
		return false, resp.Error()
	}

	return true, nil
}

// SettingsAlter
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
func (c *Connection) SettingsAlter(timeout time.Duration, index string, settings base.JsonParam) (ok bool, err error) {
	resp, err := c.request(timeout, http.MethodPut, "/"+index+"/_settings", base.Bytes2String(settings.JsonMarshal()))
	if err != nil {
		return
	}

	if !resp.IsOk() {
		return false, resp.Error()
	}

	return true, nil
}

func (c *Connection) SetMaxResultWindow(timeout time.Duration, index string, value int64) (ok bool, err error) {
	return c.SettingsAlter(timeout, index, base.JsonParam{"index.max_result_window": value})
}

// MappingsAlter
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html
func (c *Connection) MappingsAlter(timeout time.Duration, index string, mappings *Mappings) (ok bool, err error) {
	resp, err := c.Put(timeout, "/"+index+"/_mapping", base.Bytes2String(mappings.Marshal()))
	if err != nil {
		return
	}

	if !resp.IsOk() {
		return false, resp.Error()
	}

	return true, nil
}

// Bulk
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
func (c *Connection) Bulk(timeout time.Duration, param string) (resp *Response, err error) {
	return c.Post(timeout, "/_bulk", param)
}

func (c *Connection) DocsBulk(timeout time.Duration, items ...BulkDoc) (resp *Response, err error) {
	if len(items) < 1 {
		return nil, errors.New("items is required")
	}

	var buf strings.Builder

	for _, item := range items {
		buf.WriteString(item.Build())
		buf.WriteByte('\n')
	}

	return c.Bulk(timeout, buf.String())
}

// DocsInsert
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
func (c *Connection) DocsInsert(timeout time.Duration, index string, row base.JsonParam) (*results.IndexResult, error) {
	var (
		id, _ = row["_id"].(string)
		path  strings.Builder
	)

	path.WriteByte('/')
	path.WriteString(index)
	path.WriteByte('/')

	if id != "" {
		path.WriteString(`_create/`)
		path.WriteString(id)
		delete(row, "_id")
	} else {
		path.WriteString(`_doc/`)
	}

	resp, err := c.Post(timeout, path.String(), base.Bytes2String(row.JsonMarshal()))
	if err != nil {
		return nil, err
	}

	if resp.IsOk() {
		return resp.UnmarshalIndexResult()
	}

	return nil, resp.Error()
}

// DocsUpdate
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
func (c *Connection) DocsUpdate(timeout time.Duration, index string, id string, doc base.JsonParam) (*results.IndexResult, error) {
	var (
		docBytes = doc.JsonMarshal()
		n        = 7 + len(docBytes) + 1
		body     strings.Builder
	)

	body.Grow(n)

	body.WriteString(`{"doc":`)
	body.Write(docBytes)
	body.WriteByte('}')

	resp, err := c.Post(timeout, "/"+index+"/_update/"+id, body.String())
	if err != nil {
		return nil, err
	}

	if resp.IsOk() {
		return resp.UnmarshalIndexResult()
	}

	return nil, resp.Error()
}

// DocsUpdateWithVersion
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
func (c *Connection) DocsUpdateWithVersion(timeout time.Duration, index string, id string, version int64, fullDoc base.JsonParam) (*results.IndexResult, error) {
	var (
		verStr = strconv.FormatInt(version, 10)
		n      = 1 + len(index) + 6 + len(id) + 9 + len(verStr) + 25
		path   strings.Builder
	)

	path.Grow(n)

	path.WriteByte('/')
	path.WriteString(index)
	path.WriteString("/_doc/")
	path.WriteString(id)
	path.WriteString("?version=")
	path.WriteString(verStr)
	path.WriteString("&version_type=external_gt")

	resp, err := c.Put(timeout, path.String(), base.Bytes2String(fullDoc.JsonMarshal()))
	if err != nil {
		return nil, err
	}

	if resp.IsOk() {
		return resp.UnmarshalIndexResult()
	}

	return nil, resp.Error()
}

// DocsGet
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
func (c *Connection) DocsGet(timeout time.Duration, index string, id string) (*results.DocumentResult, error) {
	resp, err := c.Get(timeout, "/"+index+"/_doc/"+id, "")
	if err != nil {
		return nil, err
	}

	if resp.IsOk() || resp.Is(http.StatusNotFound) {
		return resp.UnmarshalDocumentResult()
	}

	return nil, resp.Error()
}

// DocsMGet
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
func (c *Connection) DocsMGet(timeout time.Duration, index string, idList ...string) (rows *results.DocumentsResult, err error) {
	param, _ := base.JsonMarshal(map[string]interface{}{
		"ids": idList,
	})

	resp, err := c.Get(timeout, "/"+index+"/_mget", base.Bytes2String(param))
	if err != nil {
		return nil, err
	}

	if resp.IsOk() {
		return resp.UnmarshalDocumentsResult()
	}
	return nil, resp.Error()
}

func (c *Connection) DocsMSet(timeout time.Duration, index string, rows ...base.JsonParam) (resp *Response, err error) {
	var (
		items = make([]BulkDoc, len(rows), len(rows))
		id    = ""
	)

	for i, row := range rows {
		id, _ = row["_id"].(string)
		if id != "" {
			delete(row, "_id")
		}
		items[i] = IndexDoc(index, id, row)
	}

	return c.DocsBulk(timeout, items...)
}

// DocsDelete
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html
func (c *Connection) DocsDelete(timeout time.Duration, index, id string) (*results.IndexResult, error) {
	resp, err := c.Delete(timeout, "/"+index+"/_doc/"+id, "")
	if err != nil {
		return nil, err
	}

	if resp.IsOk() || resp.Is(http.StatusNotFound) {
		return resp.UnmarshalIndexResult()
	}

	return nil, resp.Error()
}

// SqlSearch
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-search-api.html#sql-search-api
func (c *Connection) SqlSearch(timeout time.Duration, sql string) (*results.SqlSearchResult, error) {
	var param strings.Builder
	n := 10 + len(sql) + 2
	param.Grow(n)
	param.WriteString(`{"query":"`)
	param.WriteString(sql)
	param.WriteString(`"}`)

	resp, err := c.Post(timeout, "/_sql", param.String())
	if err != nil {
		return nil, err
	}

	if resp.IsOk() {
		return resp.UnmarshalSqlSearchResult()
	}

	return nil, resp.Error()
}

// SqlTranslate
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-translate-api.html
func (c *Connection) SqlTranslate(timeout time.Duration, sql string, limit int) (*Response, error) {
	var (
		param    strings.Builder
		limitStr = strconv.Itoa(limit)
	)
	n := 10 + len(sql) + 16 + len(limitStr) + 1

	param.Grow(n)
	param.WriteString(`{"query":"`)
	param.WriteString(sql)
	param.WriteString(`", "fetch_size":`)
	param.WriteString(limitStr)
	param.WriteByte('}')

	return c.Post(timeout, "/_sql/translate", param.String())
}
