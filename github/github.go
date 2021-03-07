package github

import (
	"bytes"
	gojson "encoding/json"
	"github.com/elek/go-utils/json"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
	"time"
)

type Processor func(data []byte, err error) error

func CallGithubApiV3(method string, url string) (*http.Response, error) {
	return CallGithubApiV3WithBody(method, url, []byte{})
}

func CallGithubApiV3WithBody(method string, url string, body []byte) (*http.Response, error) {
	client := &http.Client{}
	log.Debug().Msgf("%s url from GITHUB api: %s ", method, url)
	var req *http.Request
	var err error
	if len(body) > 0 {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(body))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "token "+GetToken())
	req.Header.Add("Accept", "application/vnd.github.antiope-preview+json")
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode > 299 {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Error().Msg("Can't read the body of the response: " + err.Error())
		} else {
			log.Error().Msgf(string(body))
		}
		return nil, errors.New(method + " url is failed (" + resp.Status + "): " + url)
	}
	return resp, nil
}

func ReadGithubApiV3(url string) ([]byte, error) {
	client := &http.Client{}
	log.Debug().Msgf("Reading url from GITHUB api: %s", url)

	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("Authorization", "token "+GetToken())
	req.Header.Add("Accept", "application/vnd.github.antiope-preview+json")
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
		return nil, errors.New("Reading url is failed (" + resp.Status + "): " + url)
	}
	rateLimitString := resp.Header.Get("X-Ratelimit-Remaining")
	rateLimit, err := strconv.Atoi(rateLimitString)
	if err == nil && rateLimit < 300 {
		resetTimestamp, err := strconv.ParseInt(resp.Header.Get("X-Ratelimit-Reset"), 10, 64)
		if err != nil {
			return body, nil
		}
		now := time.Now()
		resetTime := time.Unix(resetTimestamp, 0)
		if resetTime.After(now) {
			log.Info().Msg("Rate-limit threshold exceeded. Current limit: " + rateLimitString + " Waiting " + strconv.Itoa(int(resetTime.Sub(now).Seconds())) + " seconds")
			time.Sleep(resetTime.Sub(now))
		}
	}
	log.Info().Msg("Rate limit: " + rateLimitString)

	return body, nil
}

func readBody(resp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

func ReadAllGithubApiV3(url string, proc Processor) error {
	urlToUse := url
	for ; ; {
		resp, err := CallGithubApiV3("GET", urlToUse)
		err = proc(readBody(resp, err))
		if err != nil {
			return err
		}
		links := parseLinkHeader(resp.Header.Get("Link"))
		if url, hasNextPage := links["next"]; !hasNextPage {
			return nil
		} else {
			log.Debug().Msg("Getting the next page " + urlToUse)
			urlToUse = url
		}
	}
}

func parseLinkHeader(header string) map[string]string {
	result := make(map[string]string)
	if header == "" {
		return result
	}
	for _, link := range strings.Split(header, ",") {
		println(link)
		parts := strings.Split(link, ";")
		url := strings.Trim(parts[0], " ><")
		key := strings.Trim(strings.Split(parts[1], "=")[1], "\"")
		result[key] = url
	}
	return result
}

func ReadGithubApiV4Query(query []byte) ([]byte, error) {
	client := &http.Client{}

	queryPayload := make(map[string]string)
	queryPayload["query"] = string(query)

	query, err := gojson.Marshal(queryPayload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", "https://api.github.com/graphql", bytes.NewReader(query))
	req.Header.Add("Authorization", "token "+GetToken())
	req.Header.Add("Accept", "application/vnd.github.antiope-preview+json")
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
		log.Error().Msg(string(body))
		return nil, errors.New("Query failed (" + resp.Status + ")")
	}
	log.Debug().Msg("X-Ratelimit-Remaining: " + resp.Header.Get("X-Ratelimit-Remaining"))
	return body, nil
}
func GetToken() string {
	token := os.Getenv("GITHUB_TOKEN")
	if token != "" {
		return token
	}
	token = getTokenFromHubConfig()
	if token != "" {
		return token
	}
	return getTokenFromGhConfig()
}

func getTokenFromHubConfig() string {
	usr, err := user.Current()
	if err != nil {
		return ""
	}
	hubConfigFile := path.Join(usr.HomeDir, ".config", "hub")
	if _, err := os.Stat(hubConfigFile); os.IsNotExist(err) {
		return ""
	}
	data, err := ioutil.ReadFile(hubConfigFile)
	if err != nil {
		return ""
	}

	hubConfig := make(map[string]interface{})
	err = yaml.Unmarshal(data, &hubConfig)
	if err != nil {
		return ""
	}
	users := json.L(json.M(hubConfig, "github.com"))
	if len(users) > 0 {
		return json.M(users[0], "oauth_token").(string)
	}
	return ""

}

func getTokenFromGhConfig() string {
	usr, err := user.Current()
	if err != nil {
		return ""
	}
	hubConfigFile := path.Join(usr.HomeDir, ".config", "gh", "config.yml")
	if _, err := os.Stat(hubConfigFile); os.IsNotExist(err) {
		return ""
	}
	data, err := ioutil.ReadFile(hubConfigFile)
	if err != nil {
		return ""
	}

	hubConfig := make(map[string]interface{})
	err = yaml.Unmarshal(data, &hubConfig)
	if err != nil {
		return ""
	}
	users := json.L(json.M(hubConfig, "github.com"))
	if len(users) > 0 {
		return json.M(users[0], "oauth_token").(string)
	}
	return ""

}
