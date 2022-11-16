package belastic

import "github.com/grpc-boot/base"

type SearchResult struct {
	Took    int64 `json:"took"`
	Timeout bool  `json:"timed_out"`
	Hits    struct {
		Total struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`

		Hits []struct {
			Index  string         `json:"_index"`
			Id     string         `json:"_id"`
			Score  float64        `json:"_score"`
			Source base.JsonParam `json:"_source"`
			Sort   []interface{}  `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

func (sr *SearchResult) ToRows() *RowsResult {
	if sr == nil {
		return nil
	}

	rows := &RowsResult{
		Rows: make([]base.JsonParam, len(sr.Hits.Hits)),
	}

	if sr.Hits.Total.Relation == "eq" {
		rows.Total = sr.Hits.Total.Value
	}

	if len(sr.Hits.Hits) > 0 {
		for index, item := range sr.Hits.Hits {
			row := item.Source
			row["_index"] = item.Index
			row["_id"] = item.Id
			row["_sort"] = item.Sort
			row["_score"] = item.Score

			rows.Rows[index] = row
		}
	}

	return rows
}
