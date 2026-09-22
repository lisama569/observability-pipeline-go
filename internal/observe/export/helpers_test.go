package export

import (
	"encoding/json"

	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
)

// decodeBatchID extracts only the batch id from an encoded envelope.
func decodeBatchID(data []byte) (string, error) {
	var env struct {
		BatchID string `json:"batch_id"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return "", err
	}
	return env.BatchID, nil
}

func encodeBatch(id string, r trace.Record) ([]byte, error) {
	return encodeBatchRecords(id, []trace.Record{r})
}
