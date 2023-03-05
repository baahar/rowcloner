package sqlclone

import (
	"database/sql"
	"fmt"
	"log"
)

type postgresDB struct {
	*sql.DB
}

type database interface {
	getRows(string, string, interface{}) ([]map[string]interface{}, error)
	insertRow(map[string][]string, map[string][]string, References, Mapping, string, map[string]interface{}) (Mapping, error)
	getTables() ([]string, error)
	getReferences() (References, error)
	getDependencyOrder() ([]string, error)
	getPrimaryKeys() (map[string][]string, error)
	getColumnsWithDefaultValues() (map[string][]string, error)
}

// get list of tables in the database
func (db postgresDB) getTables() ([]string, error) {
	var query = "" +
		"SELECT table_name  " +
		"FROM information_schema.tables " +
		"WHERE table_schema = 'public'"

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}

// get all references from all tables
func (db postgresDB) getReferences() (References, error) {
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

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	references := make(References)
	for rows.Next() {
		var t, tc, rt, rtc string
		if err := rows.Scan(&t, &tc, &rt, &rtc); err != nil {
			log.Fatal(err)
		}
		references[t] = append(references[t], TableReference{table_name: t, column_name: tc, referenced_table_name: rt, referenced_column_name: rtc})
	}
	return references, nil
}

// get all tables that have primary keys and their primary keys
func (db postgresDB) getPrimaryKeys() (map[string][]string, error) {
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
		return nil, err
	}
	defer rows.Close()

	primary_keys := make(map[string][]string, 0)
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			log.Fatal(err)
		}
		primary_keys[t] = append(primary_keys[t], c)
	}
	return primary_keys, nil
}

// get all tables that have columns with auto values and their columns
func (db postgresDB) getColumnsWithDefaultValues() (map[string][]string, error) {
	var query = "" +
		"SELECT c.table_name, c.column_name " +
		"FROM information_schema.columns c " +
		"INNER JOIN information_schema.tables t " +
		"ON c.table_name = t.table_name " +
		"WHERE c.column_default IS NOT NULL " +
		"AND t.table_schema = 'public'"

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	auto_values := make(map[string][]string, 0)
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			return nil, err
		}
		auto_values[t] = append(auto_values[t], c)
	}
	return auto_values, nil
}

// Returns the list of tables after a topological sort following Kahn's algorithm.
// This list will be used to perform cloning so that data is inserted into the target database
// before it is needed by referencing rows later on
func (db postgresDB) getDependencyOrder() ([]string, error) {
	references, err := db.getReferences()
	if err != nil {
		return nil, err
	}

	tables, err := db.getTables()
	if err != nil {
		return nil, err
	}

	visited := make([]string, 0)
	order := make([]string, 0)
	S := make([]string, 0)
	out_degrees := make(map[string]int, 0)

	for _, table := range tables {
		ref_tables := getReferencesFromTable(references, table)
		out_degrees[table] = len(ref_tables)

		self_referencing, _ := isTableSelfReferencing(references, table)
		if self_referencing {
			out_degrees[table]--
		}

		if out_degrees[table] == 0 {
			S = append(S, table)
		}
	}

	for len(S) != 0 {
		table := S[len(S)-1]
		order = append(order, table)
		visited = append(visited, table)

		S = S[:len(S)-1] // remove table from S
		edges := getReferencesToTable(references, table)
		for _, r := range edges {
			out_degrees[r.table_name]--
			if out_degrees[r.table_name] == 0 && !sliceContains(visited, r.table_name) {
				S = append(S, r.table_name)
			}
		}
	}

	return order, nil
}

// get rows from a table where a column has a certain value
func (db postgresDB) getRows(table_name string, col string, val interface{}) ([]map[string]interface{}, error) {
	ret := make([]map[string]interface{}, 0)

	if val != nil {
		fmt.Println("SELECT * FROM " + table_name + " WHERE " + col + "='" + fmt.Sprintf("%v", val) + "'")
		rows, err := db.Query("SELECT * FROM "+table_name+" WHERE "+col+"=$1", val)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		for rows.Next() {
			colVals := make([]interface{}, len(cols))
			for i := range colVals {
				colVals[i] = new(interface{})
			}
			err = rows.Scan(colVals...)
			if err != nil {
				return nil, err
			}
			colNames, err := rows.Columns()
			if err != nil {
				return nil, err
			}
			these := make(map[string]interface{})
			for idx, name := range colNames {
				these[name] = *colVals[idx].(*interface{})
			}
			ret = append(ret, these)
		}
	}
	return ret, nil
}

// to clone a row to the target database and update the mapping if necessary
func (db postgresDB) insertRow(primary_keys map[string][]string, auto_values map[string][]string, references References, mapping Mapping, table_name string, data map[string]interface{}) (Mapping, error) {
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
		d, exists := getReference(references[table_name], key)
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
		fmt.Println(query)
		fmt.Println(values_array)

		lastInsertId := -1
		err := db.QueryRow(query, values_array...).Scan(&lastInsertId)
		if err != nil {
			return nil, err
		}

		// update mapping
		ids, exists := mapping[table_name]
		if exists {
			ids[fmt.Sprintf("%v", data["id"])] = fmt.Sprintf("%d", lastInsertId)
		} else {
			// first entry
			mapping[table_name] = map[string]string{fmt.Sprintf("%v", data["id"]): fmt.Sprintf("%d", lastInsertId)}
		}
	} else {
		fmt.Println(query)
		fmt.Println(values_array)

		_, err := db.Exec(query, values_array...)
		if err != nil {
			return nil, err
		}
	}

	return mapping, nil
}

func isTableSelfReferencing(references References, table_name string) (bool, string) {
	from := getReferencesFromTable(references, table_name)
	for _, d := range from {
		if d.referenced_table_name == table_name {
			return true, d.referenced_column_name
		}
	}
	return false, ""
}

func getReferencesToTable(references References, table_name string) []TableReference {
	var ret = make([]TableReference, 0)
	for _, value := range references {
		for _, d := range value {
			if d.referenced_table_name == table_name {
				ret = append(ret, d)
			}
		}
	}
	return ret
}

func getReferencesFromTable(references References, table_name string) []TableReference {
	return references[table_name]
}

func getReference(input []TableReference, searchString string) (TableReference, bool) {
	var ret TableReference
	for _, d := range input {
		if d.column_name == searchString {
			return d, true
		}
	}
	return ret, false
}
