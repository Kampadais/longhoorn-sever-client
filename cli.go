package main

import (
	"github.com/urfave/cli"
	_ "net/http/pprof"
	"os"
)

func main() {
	//start the pprof server

	//create a new cli
	app := cli.NewApp()

	//set the name of the cli
	app.Name = "cli"

	//set the usage of the cli
	app.Usage = "cli is a simple command line interface"

	//set the version of the cli
	app.Version = "1.0.0"

	//set the commands of the cli
	app.Commands = []cli.Command{
		{
			Name:  "client",
			Usage: "client is a simple client",
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:     "replica-streams",
					Required: false,
					Value:    1,
					Usage:    "Number of concurrent streams to each replica",
				},
				cli.BoolFlag{
					Name:     "profiling",
					Required: false,
					Usage:    "Enable pprof server",
				},
				cli.IntFlag{
					Name:     "threads",
					Required: false,
					Value:    1,
					Usage:    "Number of threads to use",
				},
				cli.BoolFlag{
					Name:     "engine",
					Required: false,
					Usage:    "Enable engine mode",
				},
			},
			Action: func(c *cli.Context) error {
				clientmain(c)
				return nil
			},
		},
		{
			Name:  "server",
			Usage: "server is a simple server",
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:     "profiling",
					Required: false,
					Usage:    "Enable pprof server",
				},
			},
			Action: func(c *cli.Context) error {
				replicamain(c)
				return nil
			},
		},
	}

	//run the cli
	err := app.Run(os.Args)
	if err != nil {
		return
	}

}
