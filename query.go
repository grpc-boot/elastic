package elastic

import (
	"strconv"
	"strings"
	"time"

	"github.com/grpc-boot/base"
)

const (
	And = ` AND `
	Or  = ` OR `
)

// Query
// link: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
type Query struct {
	fields string
	index  string
	offset int64
	after  string
	size   int
	where  string
	prefix string
	order  string
}

func (q *Query) Select(fields ...string) *Query {
	q.fields = joinWithQuote(fields, ",")
	return q
}

func (q *Query) From(index string) *Query {
	q.index = index
	return q
}

func (q *Query) Offset(offset int64) *Query {
	q.offset = offset
	return q
}

func (q *Query) Limit(size int) *Query {
	q.size = size
	return q
}

func (q *Query) After(sortItems ...interface{}) *Query {
	q.after, _ = base.JsonEncode(sortItems)
	return q
}

func (q *Query) Where(condition Condition) *Query {
	q.where = condition.Build()
	return q
}

func (q *Query) WhereString(where string) *Query {
	q.where = where
	return q
}

func (q *Query) OrderBy(order ...OrderBy) *Query {
	q.order = Order(order).toString()
	return q
}

func (q *Query) Build() string {
	size := q.size
	if size == 0 {
		size = 10
	}

	where := q.where
	if where == "" {
		where = "*"
	}

	var (
		offsetStr = strconv.FormatInt(q.offset, 10)
		sizeStr   = strconv.Itoa(size)
		n         = 35 + len(where) + 11 + len(offsetStr) + len(q.after) + 8 + len(sizeStr) + 9 + len(q.order) + 2
		buf       = strings.Builder{}
	)

	if len(q.fields) > 0 {
		n += 12 + len(q.fields) + 2
	}

	if len(q.after) > 0 {
		n += 16
	}

	buf.Grow(n)
	buf.WriteByte('{')
	if len(q.fields) > 0 {
		buf.WriteString(`"_source": [`)
		buf.WriteString(q.fields)
		buf.WriteString(`],`)
	}

	buf.WriteString(`"query":{"query_string":{"query":"`)
	buf.WriteString(where)
	buf.WriteString(`"}},"from":`)
	buf.WriteString(offsetStr)

	if len(q.after) > 0 {
		buf.WriteString(`,"search_after":`)
		buf.WriteString(q.after)
	}

	buf.WriteString(`,"size":`)
	buf.WriteString(sizeStr)

	buf.WriteString(`,"sort":[`)
	buf.WriteString(q.order)
	buf.WriteString(`]}`)

	return buf.String()
}

func (q *Query) Search(timeout time.Duration, conn *Connection) (result *SearchResult, err error) {
	param := q.Build()
	resp, err := conn.Get(timeout, "/"+q.index+"/_search", param)
	if err != nil {
		return nil, err
	}

	if resp.IsOk() {
		return resp.UnmarshalSearchResult()
	}

	return nil, resp.Error()
}

func (q *Query) SearchRows(timeout time.Duration, conn *Connection) (result *RowsResult, err error) {
	rs, err := q.Search(timeout, conn)
	if err != nil {
		return nil, err
	}

	return rs.ToRows(), nil
}
