package sqlclone

type TableReference struct {
	table_name             string
	column_name            string
	referenced_table_name  string
	referenced_column_name string
}
