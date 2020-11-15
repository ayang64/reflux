package monitor

import (
	"os"

	"github.com/ayang64/reflux/monitor/diagnostics"
)

// network captures network diagnostics.
type network struct{}

func (n *network) Diagnostics() (*diagnostics.Diagnostics, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	d := map[string]interface{}{
		"hostname": h,
	}

	return diagnostics.RowFromMap(d), nil
}
