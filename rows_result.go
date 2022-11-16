package belastic

import "github.com/grpc-boot/base"

type RowsResult struct {
	Total int64            `json:"total"`
	Rows  []base.JsonParam `json:"list"`
}
