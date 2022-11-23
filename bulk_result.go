package elastic

type BulkResult struct {
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		Index   string `json:"_index"`
		Type    string `json:"_type"`
		Id      string `json:"_id"`
		Status  int    `json:"status"`
		Result  string `json:"result"`
		Version int    `json:"_version"`
		Error   struct {
			Type    string `json:"type"`
			Reason  string `json:"reason"`
			CauseBy struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"caused_by"`
		} `json:"error"`
	}
}

func (br *BulkResult) HasErrors() bool {
	return br.Errors
}
