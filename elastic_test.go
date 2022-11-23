package elastic

import (
	"math/rand"
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

func TestConnection_IndexCreate(t *testing.T) {
	set := &Settings{
		NumberOfShards:   2,
		NumberOfReplicas: 1,
	}

	mappings := &Mappings{}
	mappings.Add(
		NewProperty(`id`, TypeUlong),
		NewProperty(`name`, TypeKeyword),
		NewProperty(`content`, TypeText),
		NewProperty(`lastLoginTime`, TypeDate).WithFormat(FormatDateTime+"||"+FormatUnixTime),
		NewProperty(`lastLoginIp`, TypeIp),
		NewProperty(`status`, TypeByte),
	)

	ok, err := conn.IndexCreate(time.Second*3, `user`, set, mappings)
	t.Logf("ok:%t err:%+v", ok, err)
}

func TestConnection_MappingsAlter(t *testing.T) {
	mappings := &Mappings{}
	mappings.Add(
		NewProperty(`tags`, TypeByte),
		NewProperty(`version`, TypeVersion),
	)

	ok, err := conn.MappingsAlter(time.Second*3, `user`, mappings)
	t.Logf("ok:%t err:%+v", ok, err)
}

func TestConnection_SetMaxResultWindow(t *testing.T) {
	ok, err := conn.SetMaxResultWindow(time.Second*3, `user`, 1000)
	t.Logf("ok:%t err:%+v", ok, err)
}

func TestConnection_IndexDelete(t *testing.T) {
	ok, err := conn.IndexDelete(time.Second*3, `user`)
	t.Logf("ok:%t err:%+v", ok, err)
}

func TestConnection_DocsInsert(t *testing.T) {
	res, err := conn.DocsInsert(time.Second*3, `user`, base.JsonParam{
		"_id":           "100",
		"name":          "name_1",
		"content":       "content user 1 user",
		"lastLoginTime": time.Now().Unix(),
		"lastLoginIp":   base.Long2Ip(rand.Uint32()),
		"status":        1,
		"tags":          []int8{1, 3, 5},
		"version":       "13.0.0.1",
	})

	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}

	t.Logf("insert custom id response:%+v", res)

	res, err = conn.DocsInsert(time.Second*3, `user`, base.JsonParam{
		"name":          "name_1",
		"content":       "content user 1 user",
		"lastLoginTime": time.Now().Unix(),
		"lastLoginIp":   base.Long2Ip(rand.Uint32()),
		"status":        1,
		"tags":          []int8{1, 3, 5},
		"version":       "13.0.0.1",
	})

	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}

	t.Logf("insert auto create id response:%+v", res)
}

func TestConnection_DocsGet(t *testing.T) {
	res, err := conn.DocsGet(time.Second*3, `user`, "100")
	if err != nil {
		t.Fatalf("want nil, got %s", err)
	}

	t.Logf("res: %+v", res)

	res, err = conn.DocsGet(time.Second*3, `user`, "1001")
	if err != nil {
		t.Fatalf("want nil, got %s", err)
	}

	t.Logf("res: %+v", res)
}

func TestConnection_DocsMGet(t *testing.T) {
	res, err := conn.DocsMGet(time.Second*3, `user`, "100", "101", "1")
	if err != nil {
		t.Fatalf("want nil, got %s", err)
	}

	t.Logf("rowsMap: %+v", res.ToRowsMap())
	t.Logf("rows: %+v", res.ToRows())
	t.Logf("res: %+v", res)
}

func TestConnection_DocsUpdate(t *testing.T) {
	getRes, err := conn.DocsGet(time.Second, `user`, "1")
	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}

	t.Logf("info:%+v", getRes)

	res, err := conn.DocsUpdate(time.Second*3, `user`, "1", base.JsonParam{
		"name": "name_100",
	})
	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}

	t.Logf("update response:%+v", res)
	tRes, err := conn.DocsGet(time.Second, `user`, res.Id)
	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}
	t.Logf("info: %+v", tRes)

	res, err = conn.DocsUpdateWithVersion(time.Second*3, `user`, res.Id, res.Version+1, getRes.Source)

	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}

	t.Logf("update response:%+v", res)

	getRes, err = conn.DocsGet(time.Second, `user`, res.Id)
	if err != nil {
		t.Fatalf("want nil, got err: %s", err)
	}
	t.Logf("info: %+v", getRes)
}

func TestConnection_BulkItems(t *testing.T) {
	resp, err := conn.DocsBulk(time.Second*10,
		IndexDoc(`user`, ``, base.JsonParam{
			"name":          "name_1",
			"content":       "content user 1 user",
			"lastLoginTime": time.Now().Unix(),
			"lastLoginIp":   base.Long2Ip(rand.Uint32()),
			"status":        1,
			"tags":          []int8{1, 3, 5},
			"version":       "13.0.0.1",
		}),
		IndexDoc(`user`, `2`, base.JsonParam{
			"name":          "name_2",
			"content":       "content user 2 user",
			"lastLoginTime": time.Now().Unix(),
			"lastLoginIp":   base.Long2Ip(rand.Uint32()),
			"status":        1,
			"tags":          []int8{1, 3, 5},
			"version":       "12.0.0.1",
		}),
		CreateDoc(`user`, base.JsonParam{
			"name":          "name_3",
			"content":       "content user 3 user",
			"lastLoginTime": time.Now().Unix(),
			"lastLoginIp":   base.Long2Ip(rand.Uint32()),
			"status":        1,
			"tags":          []int8{2, 5},
			"version":       "12.0.1.1",
		}),
	)

	if err != nil {
		t.Fatalf("want nil, got error:%s", err)
	}

	t.Logf("status: %d body:%s", resp.Status, resp.Body)

	requestOk := resp.IsOk()
	if !requestOk {
		t.Fatalf("want true, got %t", requestOk)
	}

	result, err := resp.UnmarshalBulkResult()
	if err != nil {
		t.Fatalf("want nil, got error:%s", err)
	}

	hasError := result.HasErrors()
	if hasError {
		t.Fatalf("want false, got %t", hasError)
	}
}

func TestConnection_MSet(t *testing.T) {
	resp, err := conn.DocsMSet(time.Second*10, `user`, base.JsonParam{
		"_id":           "1",
		"name":          "name_1",
		"content":       "content user 1 user",
		"lastLoginTime": time.Now().Unix(),
		"lastLoginIp":   base.Long2Ip(rand.Uint32()),
		"status":        1,
		"tags":          []int8{1, 3, 5},
		"version":       "13.0.0.1",
	},
		base.JsonParam{
			"_id":           "2",
			"name":          "name_2",
			"content":       "content user 2 user",
			"lastLoginTime": time.Now().Unix(),
			"lastLoginIp":   base.Long2Ip(rand.Uint32()),
			"status":        1,
			"tags":          []int8{1, 3, 5},
			"version":       "12.0.0.1",
		},
		base.JsonParam{
			"name":          "name_3",
			"content":       "content user 3 user",
			"lastLoginTime": time.Now().Unix(),
			"lastLoginIp":   base.Long2Ip(rand.Uint32()),
			"status":        1,
			"tags":          []int8{2, 5},
			"version":       "12.0.1.1",
		})

	if err != nil {
		t.Fatalf("want nil, got error:%s", err)
	}

	t.Logf("status: %d body:%s", resp.Status, resp.Body)

	requestOk := resp.IsOk()
	if !requestOk {
		t.Fatalf("want true, got %t", requestOk)
	}

	result, err := resp.UnmarshalBulkResult()
	if err != nil {
		t.Fatalf("want nil, got error:%s", err)
	}

	hasError := result.HasErrors()
	if hasError {
		t.Fatalf("want false, got %t", hasError)
	}
}

func TestConnection_SqlTranslate(t *testing.T) {
	resp, err := conn.SqlTranslate(time.Second*3, "SELECT COUNT(id) AS num FROM `user` GROUP BY kind", 10)
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
			Term("checkstatus", "2"),
			Term("isdel", "0"),
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
			Term("isdel", "1"),
		)).
		Limit(10).
		OrderBy(Asc("id"), Desc("color"))

	t.Logf("query:%s", query.Build())

	result, err := query.Search(time.Second*3, conn)

	t.Logf("result: %+v error:%v", result, err)
}
