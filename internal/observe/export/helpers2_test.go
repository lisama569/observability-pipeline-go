package export

import (
	"github.com/lisama569/observability-pipeline-go/internal/observe/trace"
	"github.com/lisama569/observability-pipeline-go/internal/observe/wire"
)

func encodeBatchRecords(id string, rs []trace.Record) ([]byte, error) {
	return wire.Encode(&wire.Batch{Version: wire.CurrentVersion, BatchID: id, Records: rs})
}
