package sqlclone

import (
	"database/sql"
	"fmt"
	"log"
	"sort"

	_ "github.com/lib/pq"
)

type References map[string][]TableReference
type DatabaseDump map[string][]map[string]interface{}
type Mapping map[string]map[string]string

func Download(cp *ConnectionParameters, options *DownloadOptions) (DatabaseDump, error) {
	from_db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cp.host, cp.port, cp.user, cp.password, cp.dbname))
	if err != nil {
		log.Fatal(err)
	}
	defer from_db.Close()

	return download(postgresDB{from_db}, options)
}

func download(db database, options *DownloadOptions) (DatabaseDump, error) {
	references, err := db.getReferences()
	if err != nil {
		return nil, err
	}

	database_dump := make(DatabaseDump)
	return getDataRecursively(db, references, database_dump, options.dont_recurse, options.start_points[0].table, options.start_points[0].column, options.start_points[0].value)
}

func Upload(cp *ConnectionParameters, data DatabaseDump) error {
	to_db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cp.host, cp.port, cp.user, cp.password, cp.dbname))
	if err != nil {
		return err
	}
	defer to_db.Close()

	return upload(postgresDB{to_db}, data)
}

func upload(db database, data DatabaseDump) error {

	order, err := db.getDependencyOrder()
	if err != nil {
		return err
	}

	references, err := db.getReferences()
	if err != nil {
		return err
	}

	primary_keys, err := db.getPrimaryKeys()
	if err != nil {
		return err
	}

	auto_values, err := db.getColumnsWithDefaultValues()
	if err != nil {
		return err
	}

	mapping := make(Mapping)
	for _, t := range order {
		ok, c := isTableSelfReferencing(references, t)
		if ok {
			sort.Slice(data[t], func(i, j int) bool {
				v1 := fmt.Sprintf("%v", data[t][i][c])
				v2 := fmt.Sprintf("%v", data[t][j][c])
				return v1 < v2
			})
		}
		for _, r := range data[t] {
			mapping, err = db.insertRow(primary_keys, auto_values, references, mapping, t, r)
		}
	}
	return err
}

func getDataRecursively(db database, references References, database_dump DatabaseDump, dont_recurse []string, table_name string, col string, val interface{}) (DatabaseDump, error) {
	if !sliceContains(dont_recurse, table_name) {
		rows, err := db.getRows(table_name, col, val)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			database_dump[table_name] = append(database_dump[table_name], r)

			var df = getReferencesFromTable(references, table_name)
			for _, d := range df {
				if !dumpContains(database_dump, d.referenced_table_name, d.referenced_column_name, r[d.column_name]) {
					getDataRecursively(db, references, database_dump, dont_recurse, d.referenced_table_name, d.referenced_column_name, r[d.column_name])
				}
			}

			var dr = getReferencesToTable(references, table_name)
			for _, d := range dr {
				if !dumpContains(database_dump, d.table_name, d.column_name, val) {
					getDataRecursively(db, references, database_dump, dont_recurse, d.table_name, d.column_name, val)
				}
			}
		}
	}
	return database_dump, nil
}

// ----- HELPER FUNCTIONS -----

func sliceContains(mySlice []string, searchString string) bool {
	for _, s := range mySlice {
		if s == searchString {
			return true
		}
	}
	return false
}

func dumpContains(database_dump DatabaseDump, table_name string, col string, val interface{}) bool {
	var rows = database_dump[table_name]
	for _, r := range rows {
		v, ok := r[col]
		if ok && v == val {
			return true
		}
	}
	return false
}
