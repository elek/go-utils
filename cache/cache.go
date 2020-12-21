package cache

import (
	"github.com/rs/zerolog/log"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

type Cache struct {
	Prefix string
}

type Getter func() ([]byte, error)

type IsCacheValid func(string) (bool, error)

func (cache *Cache) ForceGet(getter Getter, key string) ([]byte, error) {
	return cache.Get(getter, key, func(s string) (bool, error) {
		return false, nil
	})
}

func (cache *Cache) Get3min(getter Getter, key string) ([]byte, error) {
	return cache.Get(getter, key, timeout3Minute)
}

func timeout3Minute(cacheFile string) (bool, error) {
	if stat, err := os.Stat(cacheFile); !os.IsNotExist(err) {
		if stat.ModTime().Add(3 * time.Minute).After(time.Now()) {
			return true, nil
		}
	}
	return false, nil
}

func (cache *Cache) Get(getter Getter, key string, cacheValidator IsCacheValid) ([]byte, error) {
	oghCache := os.Getenv(strings.ToUpper(cache.Prefix) + "_CACHE")

	if oghCache == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			oghCache = path.Join(home, ".cache", strings.ToLower(cache.Prefix))
		}
	}
	cacheFile := ""
	if oghCache != "" {
		cacheFile = path.Join(oghCache, key)
	}

	if cacheFile != "" {
		_ = os.MkdirAll(oghCache, 0700)

		valid, err := cacheValidator(cacheFile)
		if err != nil {
			println("Couldn't validate cache file " + cacheFile + " " + err.Error())
		}
		if err == nil && valid {
			log.Debug().Msgf("'%s' is read from the cache", key)
			return ioutil.ReadFile(cacheFile)
		}
	}
	result, err := getter()
	if err == nil && cacheFile != "" {
		err = ioutil.WriteFile(cacheFile, result, 0600)
		if err != nil {
			return nil, err
		}
	}
	return result, err
}
