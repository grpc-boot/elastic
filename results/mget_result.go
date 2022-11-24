package results

import (
	"github.com/grpc-boot/base"
)

type MGetResult struct {
	Docs []DocumentItem `json:"docs"`
}

func (mgr *MGetResult) ToRowsMap() map[string]base.JsonParam {
	if len(mgr.Docs) < 1 {
		return nil
	}

	rows := make(map[string]base.JsonParam, len(mgr.Docs))
	for _, doc := range mgr.Docs {
		if !doc.Found {
			continue
		}

		rows[doc.Id] = doc.Source
	}

	return rows
}

func (mgr *MGetResult) ToRows() []base.JsonParam {
	if len(mgr.Docs) < 1 {
		return nil
	}

	rows := make([]base.JsonParam, 0, len(mgr.Docs))
	for _, doc := range mgr.Docs {
		if !doc.Found {
			continue
		}

		rows = append(rows, doc.Source)
	}

	return rows
}

type DocumentItem struct {
	DocumentHeader
	Found  bool           `json:"found"`
	Source base.JsonParam `json:"_source"`
}
