package results

type IndexResult struct {
	DocumentHeader

	SeqNo       int64  `json:"_seq_no"`
	PrimaryTerm int64  `json:"_primary_term"`
	Shards      Shards `json:"_shards"`
	Result      string `json:"result"`
}
