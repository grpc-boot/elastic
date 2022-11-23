package elastic

type DocumentHeader struct {
	Index       string `json:"_index"`
	Id          string `json:"_id"`
	Version     int64  `json:"_version"`
	SeqNo       int64  `json:"_seq_no"`
	PrimaryTerm int64  `json:"_primary_term"`
}
