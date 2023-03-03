package sqlclone

import (
	"database/sql"
	"fmt"
	"log"
	"sort"

	_ "github.com/lib/pq"
)

type Dependencies map[string][]Dependency
type DatabaseDump map[string][]map[string]interface{}
type Mapping map[string]map[string]string

func Download(cp *ConnectionParameters, options *DownloadOptions) (DatabaseDump, Dependencies, []string) {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cp.host, cp.port, cp.user, cp.password, cp.dbname)

	from_db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer from_db.Close()

	dependencies := getTableDependencies(*from_db)

	/*for key, value := range dependencies {
		fmt.Printf("%v: %v\n", key, value)
	}*/

	order := make([]string, 0)
	visited := make([]string, 0)
	order = updateDependencyOrder(dependencies, options.start_points[0].table, options.dont_recurse, order, visited)

	fmt.Println(order)

	database_dump := make(DatabaseDump)
	database_dump = getData(*from_db, dependencies, database_dump, options.dont_recurse, options.start_points[0].table, options.start_points[0].column, options.start_points[0].value)

	/*for key, value := range database_dump {
		fmt.Println(key)
		for _, c := range value {
			fmt.Printf("  %v \n", c)
		}
	}*/
	return database_dump, dependencies, order
}

func getData(db sql.DB, dependencies Dependencies, database_dump DatabaseDump, dont_recurse []string, table_name string, col string, val interface{}) DatabaseDump {
	// to do check if table_name is in black list
	if !sliceContains(dont_recurse, table_name) {

		rows := getRowsFromDB(db, table_name, col, val)

		for _, r := range rows {
			database_dump[table_name] = append(database_dump[table_name], r)

			var df = getReferencesFromTable(dependencies, table_name)
			for _, d := range df {
				if !dumpContains(database_dump, d.referenced_table_name, d.referenced_column_name, r[d.column_name]) {
					getData(db, dependencies, database_dump, dont_recurse, d.referenced_table_name, d.referenced_column_name, r[d.column_name])
				}
			}

			var dr = getReferencesToTable(dependencies, table_name)
			for _, d := range dr {
				if !dumpContains(database_dump, d.table_name, d.column_name, val) {
					getData(db, dependencies, database_dump, dont_recurse, d.table_name, d.column_name, val)
				}
			}
		}
	}
	return database_dump
}

func Upload(cp *ConnectionParameters, dependencies Dependencies, data DatabaseDump, order []string) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cp.host, cp.port, cp.user, cp.password, cp.dbname)

	to_db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer to_db.Close()

	primary_keys := getPrimaryKeys(*to_db)
	auto_values := getColumnsWithDefaultValues(*to_db)

	mapping := make(Mapping)
	for _, t := range order {
		// ToDo: may need to sort the rows by the referenced column? is it not in the order of the get requests already?
		//       having multiple starting points could result in rows that are out of order
		ok, c := isSelfReferencing(dependencies, t)
		if ok {
			sort.Slice(data[t], func(i, j int) bool {
				v1 := fmt.Sprintf("%v", data[t][i][c])
				v2 := fmt.Sprintf("%v", data[t][j][c])
				return v1 < v2
			})
		}
		for _, r := range data[t] {
			fmt.Println("insert from " + t)
			mapping = insertRowToDB(*to_db, primary_keys, auto_values, dependencies, mapping, t, r)
			for key, value := range mapping {
				fmt.Println(key, value)
			}
		}
	}

}

// --------------- HELPER FUNCTIONS -----------------

func getTableDependencies(db sql.DB) Dependencies {
	dependencies := make(Dependencies)

	var query = "" +
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
	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var t, tc, rt, rtc string
		if err := rows.Scan(&t, &tc, &rt, &rtc); err != nil {
			log.Fatal(err)
		}
		dependencies[t] = append(dependencies[t], Dependency{table_name: t, column_name: tc, referenced_table_name: rt, referenced_column_name: rtc})
		//fmt.Println(t + " " + tc + " " + rt + " " + rtc)
	}

	return dependencies
}

// get all tables that have primary keys and their primary keys
func getPrimaryKeys(db sql.DB) map[string][]string {
	primary_keys := make(map[string][]string, 0)

	var query = "" +
		"SELECT tc.table_name, kc.column_name " +
		"FROM " +
		"information_schema.table_constraints tc, " +
		"information_schema.key_column_usage kc " +
		"WHERE " +
		"tc.constraint_type = 'PRIMARY KEY' " +
		"AND tc.table_schema = 'public' " +
		"AND kc.table_name = tc.table_name and kc.table_schema = tc.table_schema " +
		"AND kc.constraint_name = tc.constraint_name"

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			log.Fatal(err)
		}
		primary_keys[t] = append(primary_keys[t], c)
		//fmt.Print(t + " : ")
		//fmt.Println(primary_keys[t])
	}
	return primary_keys
}

// get all tables that have columns with auto values and their columns
func getColumnsWithDefaultValues(db sql.DB) map[string][]string {
	auto_values := make(map[string][]string, 0)

	var query = "" +
		"SELECT c.table_name, c.column_name " +
		"FROM information_schema.columns c " +
		"INNER JOIN information_schema.tables t " +
		"ON c.table_name = t.table_name " +
		"WHERE c.column_default IS NOT NULL " +
		"AND t.table_schema = 'public'"

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			log.Fatal(err)
		}
		auto_values[t] = append(auto_values[t], c)
		//fmt.Println(t + " " + c)
	}
	return auto_values
}

func getReferencesToTable(dependencies Dependencies, table_name string) []Dependency {
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

func getReferencesFromTable(dependencies Dependencies, table_name string) []Dependency {
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

func isSelfReferencing(dependencies Dependencies, table_name string) (bool, string) {
	from := getReferencesFromTable(dependencies, table_name)
	for _, d := range from {
		if d.referenced_table_name == table_name {
			return true, d.referenced_column_name
		}
	}
	return false, ""
}

func updateDependencyOrder(dependencies Dependencies, table_name string, dont_recurse []string, order []string, visited []string) []string {
	if !sliceContains(dont_recurse, table_name) && !sliceContains(visited, table_name) && !sliceContains(order, table_name) {
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
		fmt.Println("SELECT * FROM " + table_name + " WHERE " + col + "='" + fmt.Sprintf("%v", val) + "'")
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

func insertRowToDB(db sql.DB, primary_keys map[string][]string, auto_values map[string][]string, dependencies Dependencies, mapping Mapping, table_name string, data map[string]interface{}) Mapping {

	query := "INSERT INTO " + table_name + " ("
	column_order := make([]string, 0)
	cols := ""
	vals := ""
	av := ""
	counter := 1
	for key := range data {
		if !sliceContains(auto_values[table_name], key) {
			cols += key + ", "
			column_order = append(column_order, key)
			vals += "$" + fmt.Sprintf("%d", counter) + ", "
			counter++
		} else {
			av = key // column that has an auto value
		}
	}
	cols = cols[:len(cols)-2]
	vals = vals[:len(vals)-2]

	values_array := make([]interface{}, 0)
	for _, key := range column_order {
		d, exists := getDependency(dependencies[table_name], key)
		if exists {
			// column contains a value that references another table
			// --> we need to use the updated value in the reference map
			if data[key] != nil {
				ids, exists := mapping[d.referenced_table_name]
				if exists {
					values_array = append(values_array, ids[fmt.Sprintf("%v", data[key])])
				} else {
					// should never be the case as we put the new ids into mapping, but just in case this would use the old value
					// todo: should we abort with error message
					values_array = append(values_array, data[key])
				}
			} else {
				values_array = append(values_array, nil)
			}
		} else {
			if data[key] != nil {
				values_array = append(values_array, data[key])
			} else {
				values_array = append(values_array, nil)
			}
		}

	}

	query += cols + ") VALUES (" + vals + ")"
	if av != "" {
		query += " RETURNING " + av
		lastInsertId := -1
		err := db.QueryRow(query, values_array...).Scan(&lastInsertId)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print("last id: ")
		fmt.Println(lastInsertId)

		// update mapping
		ids, exists := mapping[table_name]
		if exists {
			ids[fmt.Sprintf("%v", data["id"])] = fmt.Sprintf("%d", lastInsertId)
		} else {
			// first entry
			mapping[table_name] = map[string]string{fmt.Sprintf("%v", data["id"]): fmt.Sprintf("%d", lastInsertId)}
		}
	} else {
		_, err := db.Exec(query, values_array...)
		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println(query)
	fmt.Println(values_array)

	return mapping
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
