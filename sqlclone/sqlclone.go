package sqlclone

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
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
	for _, sp := range options.start_points {
		database_dump, err = getDataRecursively(db, references, database_dump, options.dont_recurse, sp.table, sp.column, sp.value)
	}
	return database_dump, err
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
			mapping, err = uploadRow(db, primary_keys, auto_values, references, mapping, t, r)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getDataRecursively(db database, references References, database_dump DatabaseDump, dont_recurse []string, table_name string, col string, val interface{}) (DatabaseDump, error) {
	if !sliceContains(dont_recurse, table_name) {
		rows, err := db.getRows(table_name, col, val)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			if !dumpContainsRow(database_dump[table_name], r) {
				database_dump[table_name] = append(database_dump[table_name], r)

				var df = getReferencesFromTable(references, table_name)
				for _, d := range df {
					if !dumpContainsResultOfQuery(database_dump, d.referenced_table_name, d.referenced_column_name, r[d.column_name]) {
						getDataRecursively(db, references, database_dump, dont_recurse, d.referenced_table_name, d.referenced_column_name, r[d.column_name])
					}
				}

				var dr = getReferencesToTable(references, table_name)
				for _, d := range dr {
					if !dumpContainsResultOfQuery(database_dump, d.table_name, d.column_name, val) {
						getDataRecursively(db, references, database_dump, dont_recurse, d.table_name, d.column_name, val)
					}
				}
			}
		}
	}
	return database_dump, nil
}

// insert a row into the target database and update the mapping if necessary
func uploadRow(db database, primary_keys map[string][]string, auto_values map[string][]string, references References, mapping Mapping, table_name string, data map[string]interface{}) (Mapping, error) {

	columns := make([]string, 0)
	av := ""
	for key := range data {
		if !sliceContains(auto_values[table_name], key) {
			columns = append(columns, key)
		} else {
			av = key // column that has an auto value
		}
	}

	values := make([]interface{}, 0)
	for _, key := range columns {
		d, exists := getReference(references[table_name], key)
		if exists {
			// column contains a value that references another table
			// --> we need to use the updated value in the reference map
			if data[key] != nil {
				ids, exists := mapping[d.referenced_table_name]
				if exists {
					values = append(values, ids[fmt.Sprintf("%v", data[key])])
				} else {
					// should never be the case as we put the new ids into mapping, but just in case this would use the old value
					// todo: should we abort with error message?
					values = append(values, data[key])
				}
			} else {
				values = append(values, nil)
			}
		} else {
			if data[key] != nil {
				values = append(values, data[key])
			} else {
				values = append(values, nil)
			}
		}

	}

	lastInsertId, err := db.insertRow(table_name, columns, values, av)
	if err != nil {
		return mapping, err
	}

	if lastInsertId != -1 {
		// update mapping
		ids, exists := mapping[table_name]
		if exists {
			ids[fmt.Sprintf("%v", data["id"])] = fmt.Sprintf("%d", lastInsertId)
		} else {
			// first entry
			mapping[table_name] = map[string]string{fmt.Sprintf("%v", data["id"]): fmt.Sprintf("%d", lastInsertId)}
		}
	}

	return mapping, nil
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

func dumpContainsResultOfQuery(database_dump DatabaseDump, table_name string, col string, val interface{}) bool {
	var rows = database_dump[table_name]
	for _, r := range rows {
		v, ok := r[col]
		if ok && v == val {
			return true
		}
	}
	return false
}

func dumpContainsRow(data []map[string]interface{}, row map[string]interface{}) bool {
	for _, currMap := range data {
		if reflect.DeepEqual(currMap, row) {
			fmt.Println(currMap)
			return true
		}
	}
	return false
}
