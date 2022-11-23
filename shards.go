package elastic

type Shards struct {
	Total      int64 `json:"total"`
	Failed     int64 `json:"failed"`
	Successful int64 `json:"successful"`
}
