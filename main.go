package main

import (
	"encoding/json"
	"fmt"
	"log"
	"main/sqlclone"
)

func main() {

	// define with which row(s) the cloning should start and which table(s) should be ignored during the process
	download_options, err := sqlclone.NewDownloadOptions(
		sqlclone.Include("purchase", "person_id", "2"),
		sqlclone.DontRecurse("user"),
	)
	if err != nil {
		log.Fatal(err)
	}

	from_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone")

	dump, dependencies, order := sqlclone.Download(from_cp, download_options)

	dataBytes, err := json.Marshal(dump)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("data:")
	fmt.Printf("%s", dataBytes)
	fmt.Println()

	to_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone_to")
	sqlclone.Upload(to_cp, dependencies, dump, order)

	fmt.Println("hallo welt")

}
