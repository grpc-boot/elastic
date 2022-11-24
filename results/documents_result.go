package results

import (
	"github.com/grpc-boot/base"
)

type DocumentsResult struct {
	Docs []DocumentResult `json:"docs"`
}

func (dr *DocumentsResult) ToRowsMap() map[string]base.JsonParam {
	if len(dr.Docs) < 1 {
		return nil
	}

	rows := make(map[string]base.JsonParam, len(dr.Docs))
	for _, doc := range dr.Docs {
		if !doc.Found {
			continue
		}

		rows[doc.Id] = doc.Source
	}

	return rows
}

func (dr *DocumentsResult) ToRows() []base.JsonParam {
	if len(dr.Docs) < 1 {
		return nil
	}

	rows := make([]base.JsonParam, 0, len(dr.Docs))
	for _, doc := range dr.Docs {
		if !doc.Found {
			continue
		}

		rows = append(rows, doc.Source)
	}

	return rows
}
