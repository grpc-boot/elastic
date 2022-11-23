package elastic

type DocumentResult struct {
	DocumentHeader

	Shards Shards `json:"_shards"`
	Result string `json:"result"`
}
