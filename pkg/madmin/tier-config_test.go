package madmin

import (
	"encoding/json"
	"testing"
)

// TestUnmarshalInvalidTierConfig tests that TierConfig parsing can catch invalid tier configs
func TestUnmarshalInvalidTierConfig(t *testing.T) {
	evilJSON := []byte(`{
      "Version": "v1",
      "Type" : "azure",
      "Name" : "GCSTIER3",
      "GCS" : {
        "Bucket" : "ilmtesting",
        "Prefix" : "testprefix3",
        "Endpoint" : "https://storage.googleapis.com/",
        "Creds": "VWJ1bnR1IDIwLjA0LjEgTFRTIFxuIFxsCgo",
        "Region" : "us-west-2",
        "StorageClass" : ""
      }
    }`)
	var tier TierConfig
	err := json.Unmarshal(evilJSON, &tier)
	if err == nil {
		t.Fatalf("expected to fail but got %v", tier)
	}
}
