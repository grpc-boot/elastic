package results

type BulkResult struct {
	Errors bool `json:"errors"`
	Items  []map[string]struct {
		DocumentHeader

		Type   string `json:"_type"`
		Status int    `json:"status"`
		Result string `json:"result"`
		Error  struct {
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
