package belastic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

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
	resp, err := c.request(timeout, http.MethodDelete, "/"+index, "")

	if err != nil {
		return
	}

	if !resp.IsOk() {
		return false, resp.Error()
	}

	return true, nil
}

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

func (c *Connection) MappingsAlter(timeout time.Duration, index string, mappings *Mappings) (ok bool, err error) {
	resp, err := c.request(timeout, http.MethodPut, "/"+index+"/_mapping", base.Bytes2String(mappings.Marshal()))
	if err != nil {
		return
	}

	if !resp.IsOk() {
		return false, resp.Error()
	}

	return true, nil
}

func (c *Connection) Bulk(timeout time.Duration, param string) (resp *Response, err error) {
	return c.Post(timeout, "/_bulk", param)
}

func (c *Connection) BulkItems(timeout time.Duration, items ...BulkItem) (resp *Response, err error) {
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

func (c *Connection) MSet(timeout time.Duration, index string, rows ...base.JsonParam) (resp *Response, err error) {
	var (
		items = make([]BulkItem, len(rows), len(rows))
		id    = ""
	)

	for i, row := range rows {
		id, _ = row["_id"].(string)
		if id != "" {
			delete(row, "_id")
		}
		items[i] = IndexItem(index, id, row)
	}

	return c.BulkItems(timeout, items...)
}

func (c *Connection) SqlTranslate(timeout time.Duration, sql string, limit int) (*Response, error) {
	query := fmt.Sprintf(`{"query": "%s", "fetch_size": %d}`, sql, limit)
	return c.Post(timeout, "/_sql/translate", query)
}
