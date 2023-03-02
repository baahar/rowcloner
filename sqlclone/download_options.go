package sqlclone

type DownloadOptions struct {
	starting_table  string
	starting_column string
	starting_value  string
	dont_recurse    string
}

// NewDownloadOptions - constructor function
func NewDownloadOptions(st string, sc string, sv string, dr string) *DownloadOptions {
	do := &DownloadOptions{
		starting_table:  st,
		starting_column: sc,
		starting_value:  sv,
		dont_recurse:    dr,
	}
	return do
}
