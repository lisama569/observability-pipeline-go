package wire

import (
	"bytes"
	"encoding/json"
)

// orderedObject is a JSON object that preserves insertion order while also
// supporting key lookup/override, used to keep wire output stable.
type orderedObject struct {
	m map[string]json.RawMessage
}

func (o *orderedObject) set(key string, val json.RawMessage) {
	if o.m == nil {
		o.m = map[string]json.RawMessage{}
	}
	o.m[key] = val
}

// marshal renders keys in a fixed known-first order, then unknown keys
// sorted, so output is deterministic.
func (o *orderedObject) marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	write := func(k string, v json.RawMessage) error {
		kb, err := json.Marshal(k)
		if err != nil {
			return err
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(v)
		return nil
	}
	for _, k := range []string{"v", "batch_id", "records"} {
		if v, ok := o.m[k]; ok {
			if err := write(k, v); err != nil {
				return nil, err
			}
		}
	}
	// Remaining keys deterministically sorted.
	rest := make([]string, 0, len(o.m))
	for k := range o.m {
		if k == "v" || k == "batch_id" || k == "records" {
			continue
		}
		rest = append(rest, k)
	}
	sortStrings(rest)
	for _, k := range rest {
		if err := write(k, o.m[k]); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func sortStrings(s []string) {
	// minimal insertion sort; unknown key counts are small.
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
