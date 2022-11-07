package s3csvtest

import (
	"bytes"
	"encoding/csv"
	"testing"
)

func MapCSVBytes(t testing.TB, data []byte) []map[string]string {
	reader := csv.NewReader(bytes.NewReader(data))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(records) < 1 {
		t.Fatalf("Need 1 record to return a map, got %d", len(records))
	}

	header := records[0]
	rows := make([]map[string]string, len(records)-1)
	for idx, src := range records[1:] {
		row := map[string]string{}
		if len(src) != len(header) {
			t.Errorf("invalid row length: %d", len(src))
			continue
		}
		for fieldIdx, headerField := range header {
			row[headerField] = src[fieldIdx]
		}
		rows[idx] = row
	}

	return rows
}

type CSVAssertion struct {
	Key        map[string]string
	Assert     map[string]string
	NotPresent bool // set to True to assert that no row matches key
}

func AssertCSV(t testing.TB, data []byte, assertions []CSVAssertion) {
	t.Helper()
	rows := MapCSVBytes(t, data)

assertionsLoop:
	for _, assertion := range assertions {
	rowLoop:
		for _, row := range rows {
			for keyField, keyVal := range assertion.Key {
				if row[keyField] != keyVal {
					continue rowLoop
				}
			}

			if assertion.NotPresent {
				t.Errorf("Unexpected row found for key %v", assertion.Key)
				continue assertionsLoop
			}

			// Matches Keys
			for valField, valWant := range assertion.Assert {
				gotVal, ok := row[valField]
				if !ok {
					t.Errorf("No value in %s for key %v", valField, assertion.Key)
					continue
				}
				if gotVal != valWant {
					t.Errorf("Field %s for key %v, want %s got %s", valField, assertion.Key, valWant, gotVal)
				}

			}

			continue assertionsLoop
		}

		if assertion.NotPresent {
			continue
		}

		t.Errorf("No row matched key %v", assertion.Key)
	}
}
