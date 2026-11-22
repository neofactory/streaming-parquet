package streaming

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// The archiver's parquet defaults match the Python writer's: zstd,
// delta on integers, BYTE_STREAM_SPLIT on floats, dictionary only on
// the named columns.
func TestArchiverDefaultEncodings(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tv", Type: arrow.PrimitiveTypes.Float64},
		{Name: "servo", Type: arrow.StructOf(
			arrow.Field{Name: "pos", Type: arrow.PrimitiveTypes.Int32})},
		{Name: "samples", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
		{Name: "count", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "label", Type: arrow.BinaryTypes.String},
		{Name: "kind", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < 100; i++ {
		b.Field(0).(*array.Float64Builder).Append(float64(i) / 2)
		sb := b.Field(1).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).Append(int32(i))
		lb := b.Field(2).(*array.ListBuilder)
		lb.Append(true)
		lb.ValueBuilder().(*array.Float32Builder).AppendValues([]float32{1, 2}, nil)
		b.Field(3).(*array.Uint16Builder).Append(uint16(i))
		b.Field(4).(*array.StringBuilder).Append("x")
		b.Field(5).(*array.StringBuilder).Append("k")
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	a := &archiver{schema: schema, compression: compress.Codecs.Zstd,
		dictionary: []string{"kind"}}
	data, err := a.encodeMini([]arrow.RecordBatch{rec})
	if err != nil {
		t.Fatal(err)
	}
	r, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	rg := r.MetaData().RowGroup(0)
	want := map[string]parquet.Encoding{
		"tv":                   parquet.Encodings.ByteStreamSplit,
		"servo.pos":            parquet.Encodings.DeltaBinaryPacked,
		"samples.list.element": parquet.Encodings.ByteStreamSplit,
		"count":                parquet.Encodings.DeltaBinaryPacked,
	}
	seen := 0
	for i := 0; i < rg.NumColumns(); i++ {
		cc, err := rg.ColumnChunk(i)
		if err != nil {
			t.Fatal(err)
		}
		path := cc.PathInSchema().String()
		if cc.Compression() != compress.Codecs.Zstd {
			t.Errorf("%s: compression %v", path, cc.Compression())
		}
		encs := map[parquet.Encoding]bool{}
		for _, e := range cc.Encodings() {
			encs[e] = true
		}
		dict := encs[parquet.Encodings.RLEDict] || encs[parquet.Encodings.PlainDict]
		if e, ok := want[path]; ok {
			seen++
			if !encs[e] {
				t.Errorf("%s: encodings %v, want %v", path, cc.Encodings(), e)
			}
		}
		if (path == "kind") != dict {
			t.Errorf("%s: dictionary %v, encodings %v", path, dict, cc.Encodings())
		}
	}
	if seen != len(want) {
		t.Errorf("found %d of the %d expected column paths", seen, len(want))
	}
}
