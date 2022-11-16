package belastic

import (
	"testing"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/zaplogger"
	"go.uber.org/zap/zapcore"
)

var (
	conn *Connection
)

func init() {
	err := base.InitZapWithOption(zaplogger.Option{
		Level: int8(zapcore.DebugLevel),
		Path:  "./log",
	})
	if err != nil {
		base.RedFatal("load log error:%s", err)
	}

	conn = New(Option{
		BaseUrl:  "http://127.0.0.1:9200",
		UserName: "user",
		Password: "123456",
	})
}

func TestConnection_SqlTranslate(t *testing.T) {
	resp, err := conn.SqlTranslate("SELECT COUNT(id) AS num FROM `user` GROUP BY kind", 10, time.Second)
	if err != nil {
		t.Fatalf("want nil, got %s", err)
	}

	t.Logf("%s", resp.Body)
}

func TestQuery_Build(t *testing.T) {
	query := Query{}

	str := query.From("user").
		Where(AndCondition(
			Gte("id", "10000"),
			Equal("checkstatus", "2"),
			Equal("isdel", "0"),
		)).
		Offset(1).
		Limit(100).
		OrderBy(Desc("created_at"), Asc("id")).
		Build()

	t.Logf(str)
}

func TestQuery_Search(t *testing.T) {
	query := Query{}

	query.Select("id", "created_at").
		From("user").
		Where(AndCondition(
			Gte("id", "10000"),
			Equal("isdel", "1"),
		)).
		Limit(10).
		OrderBy(Asc("id"), Desc("color"))

	t.Logf("query:%s", query.Build())

	result, err := query.Search(conn, time.Second*2)

	t.Logf("result: %+v error:%v", result, err)
}
