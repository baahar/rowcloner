package main

import (
	"encoding/json"
	"fmt"
	"log"
	"main/sqlclone"
)

func main() {

	dopt := sqlclone.NewDownloadOptions("company", "id", "10", "user")

	from_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone")
	dump, dependencies, order := sqlclone.Download(from_cp, dopt)

	dataBytes, err := json.Marshal(dump)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("data:")
	fmt.Printf("%s", dataBytes)

	to_cp := sqlclone.NewConnectionParameters("localhost", 5432, "baay", "deneme", "db_sqlclone_to")
	sqlclone.Upload(to_cp, dependencies, dump, order)

	fmt.Println("hallo welt")

}
