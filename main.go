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
		sqlclone.Include("person", "legal_name", "Alice"),
		sqlclone.DontRecurse("user"),
	)
	if err != nil {
		log.Fatal(err)
	}

	from_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone")

	dump, err := sqlclone.Download(from_cp, download_options)
	if err != nil {
		log.Fatal(err)
	}

	dataBytes, err := json.Marshal(dump)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("data:")
	fmt.Printf("%s", dataBytes)
	fmt.Println()

	to_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone_to")
	err = sqlclone.Upload(to_cp, dump)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("hallo welt")

}
