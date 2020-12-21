package github

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseLinkHeader(t *testing.T) {
	res := parseLinkHeader("<https://api.github.com/organizations/47359/repos?per_page=100&page=20>; rel=\"prev\", <https://api.github.com/organizations/47359/repos?per_page=100&page=1>; rel=\"first\"")
	assert.Equal(t, "https://api.github.com/organizations/47359/repos?per_page=100&page=20", res["prev"])
}
