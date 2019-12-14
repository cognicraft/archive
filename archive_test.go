package archive

import (
	"reflect"
	"testing"
)

func TestResourceString(t *testing.T) {
	tests := []struct {
		name string
		in   Resource
		out  string
	}{
		{
			name: "none",
			in:   Resource{},
			out:  "",
		},
		{
			name: "simple",
			in: Resource{
				ID: "/",
				Attributes: Attributes{
					AttributeType: TypeTextPlain,
				},
				Data: []byte("foo"),
			},
			out: "RESOURCE /\r\nType: text/plain\r\n\r\nfoo\r\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.in.String()
			if test.out != got {
				t.Errorf("expected:\n%sgot:\n%s", test.out, got)
			}
		})
	}
}

func TestAttributesString(t *testing.T) {
	tests := []struct {
		name string
		in   Attributes
		out  string
	}{
		{
			name: "single",
			in: Attributes{
				"Foo": "Bar",
			},
			out: "Foo: Bar\r\n",
		},
		{
			name: "sorted",
			in: Attributes{
				"Foo":   "Bar",
				"Azimo": "Sony",
			},
			out: "Azimo: Sony\r\nFoo: Bar\r\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.in.String()
			if test.out != got {
				t.Errorf("expected:\n%s, got:\n%s", test.out, got)
			}
		})
	}
}

func TestParseAttributes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		out  Attributes
		err  error
	}{
		{
			name: "single",
			in:   "Foo: Bar\r\n",
			out: Attributes{
				"Foo": "Bar",
			},
			err: nil,
		},
		{
			name: "sorted",
			in:   "Azimo: Sony\r\nFoo: Bar\r\n",
			out: Attributes{
				"Foo":   "Bar",
				"Azimo": "Sony",
			},
			err: nil,
		},
		{
			name: "skip-invalid",
			in:   "Azimo: Sony\r\n: Smoo\r\nBazz\r\nFoo: Bar\r\n",
			out: Attributes{
				"Foo":   "Bar",
				"Azimo": "Sony",
			},
			err: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseAttributes(test.in)
			if err != nil && test.err == nil {
				t.Errorf("expected no error but got: %s", err)
			}
			if !reflect.DeepEqual(test.out, got) {
				t.Errorf("expected: %v, got: %v", test.out, got)
			}
		})
	}
}

func TestArchive(t *testing.T) {
	a, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	if rev := a.Revision(); rev != 0 {
		t.Fatalf("expected revision to be %d but was %d", 0, rev)
	}
	_, err = a.Load("/")
	if err == nil {
		t.Fatalf("expected archive to be empty")
	}
	if rev := a.Revision(); rev != 0 {
		t.Fatalf("expected revision to be %d but was %d", 0, rev)
	}

	// store & load resource

	res := TextPlain("/", "this is a plain text")
	err = a.Store(res)
	if err != nil {
		t.Fatalf("expected store to succeed: %s", err)
	}
	if rev := a.Revision(); rev != 1 {
		t.Fatalf("expected revision to be %d but was %d", 1, rev)
	}
	sRes, err := a.Load("/")
	if err != nil {
		t.Fatalf("expected load to succeed: %s", err)
	}
	if !reflect.DeepEqual(res.Data, sRes.Data) {
		t.Fatalf("expected:\n%v\ngot:\n%v", res.Data, sRes.Data)
	}
	if rev := a.Revision(); rev != 1 {
		t.Fatalf("expected revision to be %d but was %d", 1, rev)
	}

	// replace resource

	res2 := TextPlain("/", "this is an updated text")
	err = a.Store(res2)
	if err != nil {
		t.Fatalf("expected store to succeed: %s", err)
	}
	if rev := a.Revision(); rev != 2 {
		t.Fatalf("expected revision to be %d but was %d", 2, rev)
	}
	sRes2, err := a.Load("/")
	if err != nil {
		t.Fatalf("expected load to succeed: %s", err)
	}
	if !reflect.DeepEqual(res2.Data, sRes2.Data) {
		t.Fatalf("expected: %v\ngot: %v", res2.Data, sRes2.Data)
	}
	if rev := a.Revision(); rev != 2 {
		t.Fatalf("expected revision to be %d but was %d", 2, rev)
	}
	// delete resource

	err = a.Delete("/")
	if err != nil {
		t.Fatalf("expected delete to succeed: %v", err)
	}
	if rev := a.Revision(); rev != 3 {
		t.Fatalf("expected revision to be %d but was %d", 2, rev)
	}
	_, err = a.Load("/")
	if err == nil {
		t.Fatalf("expected load to fail")
	}

}
