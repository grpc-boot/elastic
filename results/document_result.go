package results

type DocumentResult struct {
	DocumentHeader

	Shards Shards `json:"_shards"`
	Result string `json:"result"`
}
