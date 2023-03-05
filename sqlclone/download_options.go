package sqlclone

import "errors"

type DownloadOptions struct {
	start_points []StartPoint
	dont_recurse []string
}

type StartPoint struct {
	table  string
	column string
	value  string
}

type DownloadOption func(*DownloadOptions)

// Constructor function
func NewDownloadOptions(opts ...DownloadOption) (*DownloadOptions, error) {
	do := &DownloadOptions{
		start_points: make([]StartPoint, 0),
		dont_recurse: make([]string, 0),
	}

	for _, opt := range opts {
		// call the option giving the instantiated *DownloadOptions as the argument
		opt(do)
	}

	if len(do.start_points) == 0 {
		return nil, errors.New("starting point for cloning is missing")
	}

	// return the modified DownloadOptions instance
	return do, nil
}

// Add a starting point for the cloning process
func Include(table string, column string, value string) DownloadOption {
	return func(do *DownloadOptions) {
		sp := StartPoint{
			table:  table,
			column: column,
			value:  value,
		}
		do.start_points = append(do.start_points, sp)
	}
}

// Specify which tables should be ignored during cloning
func DontRecurse(table string) DownloadOption {
	return func(do *DownloadOptions) {
		do.dont_recurse = append(do.dont_recurse, table)
	}
}
