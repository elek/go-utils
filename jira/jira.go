package jira

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/user"
)

//https://docs.atlassian.com/software/jira/docs/api/REST/8.13.2/#api/2/issue-getIssue
type Jira struct {
	Url  string
	User string
}

func (jira *Jira) CreateRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)

	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	basicAuth := jira.getUser() + ":" + jira.getToken()
	req.Header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(basicAuth)))
	return req, nil
}

func (jira *Jira) getToken() string {
	return os.Getenv("JIRA_TOKEN")
}

func (jira *Jira) DoTransition(jiraId string, transitionId string, updated map[string]interface{}) ([]byte, error) {
	queryUrl := jira.Url + "/rest/api/2/issue/" + jiraId + "/transitions"

	client := &http.Client{}
	log.Debug().Msgf("%s url from JIRA api: %s %s ", "GET", jira.Url, queryUrl)

	requestBody := make(map[string]interface{})
	requestBody["update"] = updated

	transition := make(map[string]string)
	transition["id"] = transitionId
	requestBody["transition"] = transition

	requestJson, err := json.MarshalIndent(requestBody, "", "  ")
	if err != nil {
		return nil, err
	}

	req, err := jira.CreateRequest("POST", queryUrl, bytes.NewReader(requestJson))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		log.Error().Msgf(string(body))
		return nil, errors.New("Reading url is failed (" + resp.Status + "): " + queryUrl)
	}
	return body, nil
}

func (jira *Jira) ListProject() ([]byte, error) {
	queryUrl := jira.Url + "/rest/api/2/project"

	client := &http.Client{}
	log.Debug().Msgf("%s url from JIRA api: %s %s ", "GET", jira.Url, queryUrl)
	req, err := jira.CreateRequest("GET", queryUrl, nil)

	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		log.Error().Msgf(string(body))
		return nil, errors.New("Reading url is failed (" + resp.Status + "): " + queryUrl)
	}
	return body, nil
}

func (jira *Jira) GetJira(id string) ([]byte, error) {
	queryUrl := jira.Url + "/rest/api/2/issue/" + id + "?expand=editmeta"

	client := &http.Client{}
	log.Debug().Msgf("%s url from JIRA api: %s %s ", "GET", jira.Url, queryUrl)
	req, err := jira.CreateRequest("GET", queryUrl, nil)

	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		log.Error().Msgf(string(body))
		return nil, errors.New("Reading url is failed (" + resp.Status + "): " + queryUrl)
	}
	return body, nil
}

func (jira *Jira) CreateJira(fields map[string]interface{}) (string, error) {
	queryUrl := jira.Url + "/rest/api/2/issue"

	client := &http.Client{}
	log.Debug().Msgf("%s url from JIRA api: %s %s ", "POST", jira.Url, queryUrl)

	requestBody := map[string]interface{}{
		"fields": fields,
	}
	requestJson, err := json.MarshalIndent(requestBody, "", "  ")
	if err != nil {
		return "", err
	}
	println(string(requestJson))
	req, err := jira.CreateRequest("POST", queryUrl, bytes.NewReader(requestJson))
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode > 299 {
		log.Error().Msgf(string(body))
		return "", errors.New("Jira creation  failed (" + resp.Status + "): " + queryUrl)
	}
	return string(body), nil
}

func (jira *Jira) GetTransitions(id string) ([]byte, error) {
	queryUrl := jira.Url + "/rest/api/2/issue/" + id + "/transitions"

	client := &http.Client{}
	log.Debug().Msgf("%s url from JIRA api: %s %s ", "GET", jira.Url, queryUrl)
	req, err := jira.CreateRequest("GET", queryUrl, nil)

	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		log.Error().Msgf(string(body))
		return nil, errors.New("Reading url is failed (" + resp.Status + "): " + queryUrl)
	}
	return body, nil
}

func (jira *Jira) ReadSearch(query string) ([]byte, error) {
	queryUrl := jira.Url + "/rest/api/2/search?"

	client := &http.Client{}

	encodedQuery := "expand=changelog%2Ccomments&fields=%2Aall&maxResults=100&jql=" + url.QueryEscape(query)
	finalUrl := queryUrl + "&" + encodedQuery
	log.Debug().Msgf("%s url from JIRA api: %s %s", "GET", finalUrl, query)

	req, err := jira.CreateRequest("GET", finalUrl, nil)

	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		log.Error().Msgf(string(body))
		return nil, errors.New("Reading url is failed (" + resp.Status + "): " + queryUrl)
	}
	return body, nil

}

func (jira *Jira) getUser() string {
	if jira.User != "" {
		return jira.User
	}
	user, err := user.Current()
	if err == nil {
		return user.Username
	}
	return ""
}
