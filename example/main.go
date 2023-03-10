package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sqlclone"
)

func main() {
	// define with which row(s) the cloning should start and which table(s) should be ignored during the process
	download_options, err := sqlclone.NewDownloadOptions(
		sqlclone.Include("purchase", "from", "2023-11-22"),
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

	newData := make(sqlclone.DatabaseDump)
	err = json.Unmarshal(dataBytes, &newData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(newData)

	to_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone_to")
	mm, err := sqlclone.Upload(to_cp, dump)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(mm)
	fmt.Println("hallo welt")

}
