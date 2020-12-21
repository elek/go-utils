package jira

import (
	"fmt"
	jsn "github.com/elek/go-utils/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadSearch(t *testing.T) {
	jira := Jira{
		Url: "https://issues.apache.org/jira",
	}

	hddsIssues, err := jsn.AsJson(jira.ReadSearch("project = HDDS"))
	assert.Nil(t, err)
	fmt.Println(hddsIssues)
}

func TestTransitions(t *testing.T) {
	jira := Jira{
		Url: "https://issues.apache.org/jira",
	}

	content, err := jira.GetTransitions("HDDS-4440")

	transitions, err := jsn.AsJson(content, err)
	assert.Nil(t, err)
	println(string(content))
	for _, transition := range jsn.L(jsn.M(transitions, "transitions")) {
		println(jsn.MS(transition, "id"))
	}
}

func TestClose(t *testing.T) {
	jira := Jira{
		Url: "https://issues.apache.org/jira",
	}

	updated := map[string]interface{}{
		"fixVersions": []interface{}{
			map[string]interface{}{
				"add":
				map[string]interface{}{
					"name": "1.1.0",
				},
			},
		},
	}

	content, err := jira.DoTransition("HDDS-4566", "5", updated)
	assert.Nil(t, err)
	println(string(content))

}
