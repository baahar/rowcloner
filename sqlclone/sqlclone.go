package sqlclone

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

type Dependencies map[string][]Dependency
type DatabaseDump map[string][]map[string]interface{}

func Download(cp *ConnectionParameters, options *DownloadOptions) (DatabaseDump, Dependencies, []string) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cp.host, cp.port, cp.user, cp.password, cp.dbname)

	from_db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer from_db.Close()

	dependencies := getTableDependencies(*from_db, options)
	for key, value := range dependencies {
		fmt.Printf("%v: %v\n", key, value)
	}

	order := make([]string, 0)
	visited := make([]string, 0)
	order = updateDependencyOrder(dependencies, options.starting_table, options.dont_recurse, order, visited)

	database_dump := make(DatabaseDump)
	getData(*from_db, dependencies, database_dump, "company", "id", "7")

	/*for key, value := range database_dump {
		fmt.Println(key)
		for _, c := range value {
			fmt.Printf("  %v \n", c)
		}
	}*/
	return database_dump, dependencies, order
}

func getData(db sql.DB, dependencies Dependencies, database_dump DatabaseDump, table_name string, col string, val interface{}) {
	// to do check if table_name is in black list
	var rows []map[string]interface{}
	rows = getRowsFromDB(db, table_name, col, val)

	for _, r := range rows {

		database_dump[table_name] = append(database_dump[table_name], r)

		var df = getDependenciesToForeignKeysOfTable(dependencies, table_name)
		for _, d := range df {
			if !dumpContains(database_dump, d.referenced_table_name, d.referenced_table_column_name, r[d.column_name]) {
				getData(db, dependencies, database_dump, d.referenced_table_name, d.referenced_table_column_name, r[d.column_name])
			}
		}

		var dr = getDependenciesThatHaveTableAsForeignKey(dependencies, table_name)
		for _, d := range dr {
			if !dumpContains(database_dump, d.table_name, d.column_name, val) {
				getData(db, dependencies, database_dump, d.table_name, d.column_name, val)
			}
		}
	}

}

type Mapping map[string]map[string]string

func Upload(cp *ConnectionParameters, dependencies Dependencies, data DatabaseDump, order []string) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cp.host, cp.port, cp.user, cp.password, cp.dbname)

	to_db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer to_db.Close()

	id_map := make(Mapping)
	for _, t := range order {
		for _, r := range data[t] {
			id_map = insertRowToDB(*to_db, dependencies, id_map, t, r)
			for key, value := range id_map {
				fmt.Println(key, value)
			}
		}
	}

}

// --------------- HELPER FUNCTIONS -----------------

func getTableDependencies(db sql.DB, options *DownloadOptions) Dependencies {
	dependencies := make(Dependencies)

	var dependence_query = "" +
		"SELECT " +
		"conrelid::regclass table_name, " +
		"a1.attname column_name, " +
		"confrelid::regclass referenced_table, " +
		"a2.attname referenced_column_name " +
		"FROM (" +
		"select conrelid::regclass, confrelid::regclass, col, fcol " +
		"from pg_constraint, " +
		"lateral unnest(conkey) col, " +
		"lateral unnest(confkey) fcol " +
		"where contype = 'f'" +
		") s " +
		"JOIN pg_attribute a1 ON a1.attrelid = conrelid AND a1.attnum = col " +
		"JOIN pg_attribute a2 ON a2.attrelid = confrelid AND a2.attnum = fcol;"

	// get dependencies
	rows, err := db.Query(dependence_query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var t, tc, rt, rtc string
		if err := rows.Scan(&t, &tc, &rt, &rtc); err != nil {
			log.Fatal(err)
		}
		dependencies[t] = append(dependencies[t], Dependency{table_name: t, column_name: tc, referenced_table_name: rt, referenced_table_column_name: rtc})
		//fmt.Println(t + " " + tc + " " + rt + " " + rtc)
	}

	return dependencies
}

func getDependenciesThatHaveTableAsForeignKey(dependencies Dependencies, table_name string) []Dependency {
	var ret = make([]Dependency, 0)
	for _, value := range dependencies {
		for _, d := range value {
			if d.referenced_table_name == table_name {
				ret = append(ret, d)
			}
		}
	}
	return ret
}

func getDependenciesToForeignKeysOfTable(dependencies Dependencies, table_name string) []Dependency {
	return dependencies[table_name]
}

func getDependency(input []Dependency, searchString string) (Dependency, bool) {
	var ret Dependency
	for _, d := range input {
		if d.column_name == searchString {
			return d, true
		}
	}
	return ret, false
}

func updateDependencyOrder(dependencies Dependencies, table_name string, dont_recurse string, order []string, visited []string) []string {
	if dont_recurse != table_name && !sliceContains(visited, table_name) && !sliceContains(order, table_name) {
		visited = append(visited, table_name)

		// if table has foreign keys, those tables have to be considered first
		for _, s := range dependencies[table_name] {
			if table_name != s.referenced_table_name {
				order = updateDependencyOrder(dependencies, s.referenced_table_name, dont_recurse, order, visited)
			}
		}

		if !sliceContains(order, table_name) {
			order = append(order, table_name)
		}

		// if table is foreign key of another table, consider them as well
		for key, value := range dependencies {
			for _, d := range value {
				if d.referenced_table_name == table_name && table_name != key {
					order = updateDependencyOrder(dependencies, key, dont_recurse, order, visited)
				}
			}
		}

	} else {
		// stop recursion
	}
	return order
}

func getRowsFromDB(db sql.DB, table_name string, col string, val interface{}) []map[string]interface{} {
	ret := make([]map[string]interface{}, 0)

	if val != nil {

		rows, err := db.Query("SELECT * FROM " + table_name + " WHERE " + col + "='" + fmt.Sprintf("%v", val) + "'")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		// code from https://gist.github.com/proprietary/b401b0f7e9fb6c00ed06df553c6a3977
		for rows.Next() {
			colVals := make([]interface{}, len(cols))
			for i := range colVals {
				colVals[i] = new(interface{})
			}
			err = rows.Scan(colVals...)
			if err != nil {
				log.Fatal(err)
			}
			colNames, err := rows.Columns()
			if err != nil {
				log.Fatal(err)
			}
			these := make(map[string]interface{})
			for idx, name := range colNames {
				these[name] = *colVals[idx].(*interface{})
			}
			ret = append(ret, these)
		}
	}
	return ret
}

func insertRowToDB(db sql.DB, dependencies Dependencies, id_map Mapping, table_name string, data map[string]interface{}) Mapping {

	query := "INSERT INTO " + table_name + " ("
	key_order := make([]string, 0)
	for key, _ := range data {
		if key != "id" {
			query += key + ", "
			key_order = append(key_order, key)
		}
	}

	query = query[:len(query)-2]
	query += ") VALUES ("

	for _, key := range key_order {

		d, exists := getDependency(dependencies[table_name], key)
		if exists {
			// column contains a value that references another table
			// --> we need to use the updated value
			if data[key] != nil {
				ids, exists := id_map[d.referenced_table_name]
				if exists {
					query += "'" + ids[fmt.Sprintf("%v", data[key])] + "', "
				} else {
					// should never be the case as we put the new ids into the id_map, but just in case this would use the old value
					// todo: should we abort with error message
					query += "'" + fmt.Sprintf("%v", data[key]) + "', "
				}
			} else {
				query += "NULL, "
			}
		} else {
			if data[key] != nil {
				query += "'" + fmt.Sprintf("%v", data[key]) + "', "
			} else {
				query += "NULL, "
			}
		}

	}

	query = query[:len(query)-2]
	query += ") RETURNING id"

	fmt.Println(query)

	// insert row
	lastInsertId := 0
	err := db.QueryRow(query).Scan(&lastInsertId)
	if err != nil {
		log.Fatal(err)
	}

	// update mapping so that old id points to new_id for future reference
	ids, exists := id_map[table_name]
	if exists {
		ids[fmt.Sprintf("%v", data["id"])] = fmt.Sprintf("%d", lastInsertId)
	} else {
		// first entry
		id_map[table_name] = map[string]string{fmt.Sprintf("%v", data["id"]): fmt.Sprintf("%d", lastInsertId)}
	}

	return id_map
}

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
