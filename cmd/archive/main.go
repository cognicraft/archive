package main

import (
	"fmt"
	"log"
	"os"

	"github.com/cognicraft/archive"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal(fmt.Errorf("no command specified"))
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	log.Printf("%s %v", cmd, args)

	switch cmd {
	case "store":
		arc := args[0]
		id := args[1]
		file := args[2]

		a, err := archive.Open(arc)
		if err != nil {
			log.Fatal(err)
		}
		defer a.Close()
		err = a.ImportFile(id, file)
		if err != nil {
			log.Fatal(err)
		}
	case "load":
		arc := args[0]
		id := args[1]
		file := args[2]

		a, err := archive.Open(arc)
		if err != nil {
			log.Fatal(err)
		}
		defer a.Close()
		err = a.ExportFile(id, file)
		if err != nil {
			log.Fatal(err)
		}
	case "delete":
		arc := args[0]
		id := args[1]

		a, err := archive.Open(arc)
		if err != nil {
			log.Fatal(err)
		}
		defer a.Close()
		err = a.Delete(id)
		if err != nil {
			log.Fatal(err)
		}
	}
}
