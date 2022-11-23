package belastic

type DocumentResult struct {
	Shards struct {
		Total      int64 `json:"total"`
		Failed     int64 `json:"failed"`
		Successful int64 `json:"successful"`
	} `json:"_shards"`
	Index       string `json:"_index"`
	Id          string `json:"_id"`
	Version     int64  `json:"_version"`
	SeqNo       int64  `json:"_seq_no"`
	PrimaryTerm int64  `json:"_primary_term"`
	Result      string `json:"result"`
}
