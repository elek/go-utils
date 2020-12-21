package incremental

import (
	"github.com/elek/go-utils/kv"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"time"
)

type Incremental struct {
	Store kv.KV
	Key   string
}

type Task func(lastUpdate time.Time) (time.Time, error)

//execute incremental task from the last updated time
func (inc *Incremental) Update(task Task) (bool, error) {
	lastUpdate := time.Unix(0, 0)
	if inc.Store.Contains(inc.Key) {
		lastUpdateString, err := inc.Store.Get(inc.Key)
		if err != nil {
			log.Warn().Msg("Can't retrieve LAST value from KV store. Initializing with 0 epoch.")
		} else {
			lastUpdate, err = time.Parse(time.RFC3339, string(lastUpdateString))
			if err != nil {
				return false, errors.Wrap(err, "LAST value is not an RFC3339 time but "+string(lastUpdateString))
			}
		}
	}

	doneUntil, err := task(lastUpdate)
	if err != nil {
		return false, errors.Wrap(err, "Execution of task is failed")
	}
	//lastUpdate will be different, work should be retried
	didWork := doneUntil.After(lastUpdate)
	err = inc.Store.Put(inc.Key, []byte(doneUntil.Format(time.RFC3339)))
	if err != nil {
		return false, errors.Wrap(err, "Couldn't store new updated time")
	}
	return didWork, nil
}
