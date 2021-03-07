package main

import (
	util "github.com/elek/go-utils"
	"github.com/elek/go-utils/kv"
	"log"
	"os"
)
import "github.com/urfave/cli/v2"

func main() {
	app := &cli.App{
		Name:  "kvcli",
		Usage: "Utility for lightweight KV stores",
		Commands: []*cli.Command{
			{
				Name:    "copy",
				Aliases: []string{"cp"},
				Usage:   "Copy keys from one kv store to an other",
				Action: func(c *cli.Context) error {
					from, err := kv.Create(c.Args().Get(0))
					if err != nil {
						return err
					}
					to, err := kv.Create(c.Args().Get(1))
					if err != nil {
						return err
					}
					copy(from, to)
					return nil
				},
			},
			{
				Name:  "count",
				Usage: "Count keys in a kv store",
				Action: func(c *cli.Context) error {
					store, err := kv.Create(c.Args().Get(0))
					if err != nil {
						return err
					}
					return count(store)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func copy(from kv.KV, to kv.KV) error {

	p := util.CreateProgress()
	err := from.IterateAll(func(key string) error {
		value, err := from.Get(key)
		if err != nil {
			return err
		}
		err = to.Put(key, value)
		if err != nil {
			return err
		}
		p.Increment()
		return nil
	})
	p.End()
	return err
}

func count(store kv.KV) error {
	counter := 0
	p := util.CreateProgress()
	err := store.IterateAll(func(key string) error {
		counter++
		p.Increment()
		return nil
	})
	if err != nil {
		return err
	}
	p.End()
	println(counter)
	return nil
}
