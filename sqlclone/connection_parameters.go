package sqlclone

type ConnectionParameters struct {
	host     string
	port     int
	user     string
	password string
	dbname   string
}

// NewConnectionParameters - constructor function
func NewConnectionParameters(h string, p int, u string, pw string, db string) *ConnectionParameters {
	cp := &ConnectionParameters{
		host:     h,
		port:     p,
		user:     u,
		password: pw,
		dbname:   db,
	}
	return cp
}
