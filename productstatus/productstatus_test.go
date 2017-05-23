package productstatus_test

import (
	"testing"

	"github.com/metno/productshredder/productstatus"
	"github.com/stretchr/testify/assert"
)

var clientTests = []struct {
	rawurl  string
	success bool
}{
	{"http://foo", true},
	{"https://productstatus.met.no", true},
	{"http://foo:1234/api/trololol/", true},
	{"//foo/", false},
	{"http:||nope", false},
}

// Test that the client is properly initialized using a multitude of URLs.
func TestNewClient(t *testing.T) {
	for _, test := range clientTests {
		_, err := productstatus.New(test.rawurl, "user", "pass")
		t.Logf(test.rawurl)
		if test.success {
			assert.Nil(t, err)
		} else {
			assert.NotNil(t, err)
		}
	}
}
