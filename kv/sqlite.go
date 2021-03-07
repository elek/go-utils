package kv

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"io"
	"path"
	"time"
)

type SqliteKV struct {
	db          *sql.DB
	prefixCache map[string]bool
}

func CreateSqliteKV(dbFile string) (*SqliteKV, error) {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, err
	}

	sqlStmt := `
	create table if not exists prefix (prefix integer not null, key text, PRIMARY KEY (prefix, key));
	create table if not exists key (prefix integer not null, key text, value text, PRIMARY KEY (prefix, key));
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		return nil, err
	}
	return &SqliteKV{
		db:          db,
		prefixCache: make(map[string]bool),
	}, nil
}
func (s SqliteKV) Put(key string, value []byte) error {
	if _, found := s.prefixCache[path.Dir(key)]; !found {
		for parent := path.Dir(key); parent != "."; parent = path.Dir(parent) {
			_, err := s.db.Exec("INSERT INTO prefix (prefix,key) VALUES (?,?) ON CONFLICT DO NOTHING", path.Dir(parent), path.Base(parent))
			if err != nil {
				return err
			}
		}
		s.prefixCache[path.Dir(key)] = true
	}
	_, err := s.db.Exec("INSERT INTO key (prefix,key,value) VALUES (?,?,?)", path.Dir(key), path.Base(key), value)
	if err != nil {
		return err
	}
	return err
}

func (s *SqliteKV) List(prefix string) ([]string, error) {
	result := make([]string, 0)
	res, err := s.db.Query("SELECT key FROM key WHERE prefix = ?", path.Base(prefix))
	if err != nil {
		return result, err
	}
	var key string
	for ; res.Next(); {
		err = res.Scan(&key)
		if err != nil {
			return result, err
		}
		result = append(result, path.Join(prefix, key))
	}
	res.Close()
	res, err = s.db.Query("SELECT key FROM prefix WHERE prefix = ?", path.Base(prefix))
	if err != nil {
		return result, err
	}
	for ; res.Next(); {
		err = res.Scan(&key)
		if err != nil {
			return result, err
		}
		result = append(result, path.Join(prefix, key))
	}
	res.Close()
	return result, nil
}

func (s *SqliteKV) IterateAll(action IteratorAction) error {
	res, err := s.db.Query("SELECT * FROM key")
	defer res.Close()
	if err != nil {
		return err
	}
	var prefix, key, value string
	for ; res.Next(); {
		err = res.Scan(&prefix, &key, &value)
		if err != nil {
			return err
		}
		err = action(path.Join(prefix, key))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SqliteKV) Iterate(prefix string, action IteratorAction) error {
	res, err := s.db.Query("SELECT key FROM key WHERE prefix = ?", path.Base(prefix))
	if err != nil {
		return err
	}
	var key string
	for ; res.Next(); {
		err = res.Scan(&key)
		if err != nil {
			return err
		}
		err = action(path.Join(prefix, key))
		if err != nil {
			return err
		}
	}
	res.Close()
	res, err = s.db.Query("SELECT key FROM prefix WHERE prefix = ?", path.Base(prefix))
	if err != nil {
		return err
	}
	for ; res.Next(); {
		err = res.Scan(&key)
		if err != nil {
			return err
		}
		err = action(path.Join(prefix, key))
		if err != nil {
			return err
		}
	}
	res.Close()
	return nil
}

func (s *SqliteKV) IterateSubTree(prefix string, action IteratorAction) error {
	panic("implement me")
}

func (s *SqliteKV) Contains(key string) bool {
	res, err := s.db.Query("SELECT * FROM key WHERE prefix = ? AND key = ?", path.Dir(key), path.Base(key))
	defer res.Close()
	if err != nil {
		return false
	}
	return res.Next()
}

func (s *SqliteKV) GetOrDefault(key string, defaultFunc Getter) ([]byte, error) {
	res, err := s.db.Query("SELECT value FROM key WHERE prefix = ? AND key = ?", path.Dir(key), path.Base(key))
	defer res.Close()
	if err != nil {
		return []byte{}, err
	}
	if res.Next() {
		var value []byte
		err = res.Scan(&value)
		if err != nil {
			return []byte{}, err
		}
		return value, nil
	} else {
		return defaultFunc(key)
	}
}

func (s *SqliteKV) Get(key string) ([]byte, error) {
	res, err := s.db.Query("SELECT value FROM key WHERE prefix = ? AND key = ?", path.Dir(key), path.Base(key))
	defer res.Close()
	if err != nil {
		return []byte{}, err
	}
	if res.Next() {
		var value []byte
		err = res.Scan(&value)
		if err != nil {
			return []byte{}, err
		}
		return value, nil
	} else {
		return []byte{}, errors.New("No such key " + key)
	}
}

func (s *SqliteKV) GetReader(prefix string) (io.Reader, error) {
	panic("implement me")
}

func (s *SqliteKV) IsChanged(since time.Time, prefix string) (bool, error) {
	panic("implement me")
}
