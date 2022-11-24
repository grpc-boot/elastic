package results

import "github.com/grpc-boot/base"

type DocumentResult struct {
	DocumentHeader

	SeqNo       int64          `json:"_seq_no"`
	PrimaryTerm int64          `json:"_primary_term"`
	Found       bool           `json:"found"`
	Source      base.JsonParam `json:"_source"`
}
