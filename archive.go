package archive

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"mime"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cognicraft/sqlutil"
	_ "github.com/mattn/go-sqlite3"
)

func Open(dsn string) (*Archive, error) {
	a := &Archive{
		dsn: dsn,
	}
	return a, a.init()
}

type Archive struct {
	dsn string

	mu sync.Mutex
	db *sql.DB
}

func (a *Archive) Revision() int {
	row := a.db.QueryRow(`SELECT VALUE FROM INFO WHERE NAME = ?;`, InfoRevision)
	revision := 0
	row.Scan(&revision)
	return revision
}

func (a *Archive) List() ([]Descriptor, error) {
	rows, err := a.db.Query(`SELECT ID, ATTRIBUTES FROM RESOURCES ORDER BY ID;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []Descriptor{}
	for rows.Next() {
		var id string
		var attributes string
		err = rows.Scan(&id, &attributes)
		if err != nil {
			return nil, err
		}
		as, err := ParseAttributes(attributes)
		if err != nil {
			return nil, err
		}
		res = append(res, Descriptor{ID: id, Attributes: as})
	}
	return res, nil
}

func (a *Archive) ListWithPrefix(prefix string) ([]Descriptor, error) {
	rows, err := a.db.Query(`SELECT ID, ATTRIBUTES FROM RESOURCES WHERE ID LIKE ? ORDER BY ID;`, prefix+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []Descriptor{}
	for rows.Next() {
		var id string
		var attributes string
		err = rows.Scan(&id, &attributes)
		if err != nil {
			return nil, err
		}
		as, err := ParseAttributes(attributes)
		if err != nil {
			return nil, err
		}
		res = append(res, Descriptor{ID: id, Attributes: as})
	}
	return res, nil
}

func (a *Archive) Attributes(id string) (Attributes, error) {
	row := a.db.QueryRow(`SELECT ATTRIBUTES FROM RESOURCES WHERE ID = ?;`, id)
	var attributes string
	err := row.Scan(&attributes)
	if err != nil {
		return nil, err
	}
	as, err := ParseAttributes(attributes)
	if err != nil {
		return nil, err
	}
	return as, nil
}

func (a *Archive) Load(id string) (Resource, error) {
	row := a.db.QueryRow(`SELECT ATTRIBUTES, DATA FROM RESOURCES WHERE ID = ?;`, id)
	var attributes string
	var data []byte
	err := row.Scan(&attributes, &data)
	if err != nil {
		return Resource{}, err
	}
	as, err := ParseAttributes(attributes)
	if err != nil {
		return Resource{}, err
	}
	res := Resource{
		ID:         id,
		Data:       data,
		Attributes: as,
	}
	return res, nil
}

func (a *Archive) Store(r Resource) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	as := r.Attributes.Clone()
	as[AttributeLength] = fmt.Sprintf("%d", len(r.Data))
	as[AttributeLastModified] = time.Now().UTC().Format(time.RFC3339)

	err := sqlutil.Transact(a.db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO RESOURCES (ID, ATTRIBUTES, DATA) VALUES (?, ?, ?);`, r.ID, as.String(), r.Data); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE INFO SET VALUE = VALUE + 1 WHERE NAME = ?;`, InfoRevision); err != nil {
			return err
		}
		return nil
	})
	return err
}

func (a *Archive) Delete(id string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	err := sqlutil.Transact(a.db, func(tx *sql.Tx) error {
		r, err := tx.Exec(`DELETE FROM RESOURCES WHERE ID=?;`, id)
		if err != nil {
			return err
		}
		if a, _ := r.RowsAffected(); a > 0 {
			if _, err := tx.Exec(`UPDATE INFO SET VALUE = VALUE + 1 WHERE NAME = ?;`, InfoRevision); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (a *Archive) ImportFile(id string, file string) error {
	bs, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	attr := Attributes{}
	if typ := mime.TypeByExtension(filepath.Ext(file)); typ != "" {
		attr[AttributeType] = typ
	}
	return a.Store(MakeResource(id, attr, bs))
}

func (a *Archive) ExportFile(id string, file string) error {
	res, err := a.Load(id)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, res.Data, 0644)
}

func (a *Archive) Close() error {
	return a.db.Close()
}

func (a *Archive) init() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	db, err := sql.Open("sqlite3", a.dsn)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS INFO (NAME TEXT, VALUE TEXT, PRIMARY KEY (NAME));`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS RESOURCES (ID TEXT, ATTRIBUTES TEXT, DATA BLOB, PRIMARY KEY (ID));`)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT OR IGNORE INTO INFO (name, value) VALUES (?, ?);`, InfoRevision, "0")
	if err != nil {
		return err
	}
	a.db = db
	return nil
}

func GenericJSON(id string, v interface{}) Resource {
	return JSON(id, TypeApplicationJSON, v)
}

func JSON(id string, t string, v interface{}) Resource {
	bs, _ := json.Marshal(v)
	return MakeResource(id, Attributes{AttributeType: t}, bs)
}

func LoadJSON(a *Archive, id string, v interface{}) error {
	res, err := a.Load(id)
	if err != nil {
		return err
	}
	err = json.Unmarshal(res.Data, v)
	if err != nil {
		return err
	}
	return nil
}

func GenericXML(id string, v interface{}) (Resource, error) {
	return XML(id, TypeApplicationXML, v)
}

func XML(id string, t string, v interface{}) (Resource, error) {
	bs, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return Resource{}, err
	}
	return MakeResource(id, Attributes{AttributeType: t}, bs), nil
}

func LoadXML(a *Archive, id string, v interface{}) error {
	res, err := a.Load(id)
	if err != nil {
		return err
	}
	err = xml.Unmarshal(res.Data, v)
	if err != nil {
		return err
	}
	return nil
}

func TextPlain(id string, text string) Resource {
	return MakeResource(id, Attributes{AttributeType: TypeTextPlain}, []byte(text))
}

func JPEG(id string, data []byte) Resource {
	return MakeResource(id, Attributes{AttributeType: TypeImageJPEG}, data)
}

func MakeResource(id string, as Attributes, data []byte) Resource {
	return Resource{
		ID:         id,
		Attributes: as,
		Data:       data,
	}
}

type Descriptor struct {
	ID         string
	Attributes Attributes
}

type Resource struct {
	ID         string
	Attributes Attributes
	Data       []byte
}

func (r Resource) String() string {
	if r.ID == "" {
		return ""
	}
	buf := &bytes.Buffer{}
	out := []string{
		"RESOURCE " + r.ID + "\r\n",
		r.Attributes.String(),
	}
	if r.Data != nil {
		out = append(out, "\r\n")
		switch r.Attributes[AttributeType] {
		case TypeTextPlain:
			out = append(out, string(r.Data))
		default:
			out = append(out, fmt.Sprintf("%v", r.Data))
		}
		out = append(out, "\r\n")
	}
	for _, s := range out {
		if _, err := buf.WriteString(s); err != nil {
			return ""
		}
	}
	return buf.String()
}

type Attributes map[string]string

func (as Attributes) String() string {
	buf := &bytes.Buffer{}
	for _, e := range as.Entries() {
		for _, s := range []string{e.Key, ": ", e.Value, "\r\n"} {
			if _, err := buf.WriteString(s); err != nil {
				return ""
			}
		}
	}
	return buf.String()
}

func (as Attributes) Entries() Entries {
	es := Entries{}
	for k, v := range as {
		es = append(es, Entry{Key: k, Value: v})
	}
	sort.Sort(es)
	return es
}

func (as Attributes) Clone() Attributes {
	as2 := make(Attributes, len(as))
	for k, v := range as {
		as2[k] = v
	}
	return as2
}

func ParseAttributes(data string) (Attributes, error) {
	as := Attributes{}
	data = strings.Replace(data, "\r\n", "\n", -1)
	for _, line := range strings.Split(data, "\n") {
		idx := strings.Index(line, ": ")
		if idx <= 0 {
			// not a valid attribute entry
			continue
		}
		key, value := line[:idx], line[idx+2:]
		as[key] = value
	}
	return as, nil
}

type Entry struct {
	Key   string
	Value string
}

type Entries []Entry

func (s Entries) Len() int           { return len(s) }
func (s Entries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Entries) Less(i, j int) bool { return s[i].Key < s[j].Key }

const (
	AttributeEncoding     = "Encoding"
	AttributeETag         = "ETag"
	AttributeExpires      = "Expires"
	AttributeLastModified = "Last-Modified"
	AttributeLabel        = "Label"
	AttributeLength       = "Length"
	AttributeType         = "Type"
)

const (
	TypeApplicationJSON = "application/json"
	TypeApplicationPDF  = "application/pdf"
	TypeApplicationXML  = "application/xml"
	TypeImageJPEG       = "image/jpeg"
	TypeImagePNG        = "image/png"
	TypeImageSVG        = "image/svg+xml"
	TypeTextCSV         = "text/csv"
	TypeTextHTML        = "text/html"
	TypeTextPlain       = "text/plain"
)

const (
	EncodingIdentity = "identity"
	EncodingGZIP     = "gzip"
)

const (
	InfoRevision = "Revision"
)
