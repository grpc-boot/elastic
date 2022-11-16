package belastic

import (
	"bytes"
	"context"
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

func (c *Connection) request(method, path string, params string, timeout time.Duration) (response *Response, err error) {
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

func (c *Connection) Put(path string, params string, timeout time.Duration) (*Response, error) {
	return c.request(http.MethodPut, path, params, timeout)
}

func (c *Connection) Post(path string, params string, timeout time.Duration) (*Response, error) {
	return c.request(http.MethodPost, path, params, timeout)
}

func (c *Connection) Get(path string, params string, timeout time.Duration) (*Response, error) {
	return c.request(http.MethodGet, path, params, timeout)
}

func (c *Connection) SqlTranslate(sql string, limit int, timeout time.Duration) (*Response, error) {
	query := fmt.Sprintf(`{"query": "%s", "fetch_size": %d}`, sql, limit)
	return c.Post("/_sql/translate", query, timeout)
}