package results

type DocumentHeader struct {
	Index   string `json:"_index"`
	Id      string `json:"_id"`
	Version int64  `json:"_version"`
}
