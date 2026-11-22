package ring

// direct.go — the per-field store without a cgo call. The core owns the
// storage and the open row; RowBegin (cgo) opens the slot, and the Go
// host then writes fixed-width values straight into the leaf's buffer
// at that slot and sets the validity bit itself, exactly as the core's
// own setters do (store_int, ar_set_f64, ar_set_bool, ar_set_bytes,
// ar_set_list in arrow_ring_core.c). Var-width leaves (strings, lists)
// keep the core's pool setters, and float16 keeps ar_set_f64.

/*
#include "arrow_ring.h"
*/
import "C"
import (
	"fmt"
	"math"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
)

type leafKind uint8

const (
	kindPrim  leafKind = iota // one fixed-width value per row
	kindBool                  // bit-packed
	kindBytes                 // fixed-width blob per row (decimal, fixed_size_binary)
	kindFSL                   // fixed_size_list of a fixed-width primitive
	kindVar                   // utf8/binary/list: the core's segment pool
)

type leafClass uint8

const (
	classSigned leafClass = iota
	classUnsigned
	classFloat
)

// leafInfo is what a direct store needs about one leaf: its storage
// shape from the schema, and the core's buffer addresses.
type leafInfo struct {
	path     string
	kind     leafKind
	class    leafClass
	itemsize int64 // prim/fsl element bytes
	width    int64 // bytes: row bytes; fsl: elements per row
	nullable bool
	data     unsafe.Pointer // the leaf's ring buffer (nil for var)
	valid    unsafe.Pointer // validity bitmap, nil when not nullable
}

// primShape classifies a fixed-width primitive type as the core does.
func primShape(t arrow.DataType) (class leafClass, itemsize int64, ok bool) {
	switch t.ID() {
	case arrow.INT8:
		return classSigned, 1, true
	case arrow.INT16:
		return classSigned, 2, true
	case arrow.INT32, arrow.DATE32, arrow.TIME32:
		return classSigned, 4, true
	case arrow.INT64, arrow.TIMESTAMP, arrow.DATE64, arrow.TIME64, arrow.DURATION:
		return classSigned, 8, true
	case arrow.UINT8:
		return classUnsigned, 1, true
	case arrow.UINT16:
		return classUnsigned, 2, true
	case arrow.UINT32:
		return classUnsigned, 4, true
	case arrow.UINT64:
		return classUnsigned, 8, true
	case arrow.FLOAT16:
		return classFloat, 2, true
	case arrow.FLOAT32:
		return classFloat, 4, true
	case arrow.FLOAT64:
		return classFloat, 8, true
	}
	return 0, 0, false
}

// flattenLeaves walks the schema the way the core does (struct fields
// become dotted paths) and answers one leafInfo per leaf path.
func flattenLeaves(fields []arrow.Field, prefix string, out map[string]*leafInfo) error {
	for _, f := range fields {
		path := f.Name
		if prefix != "" {
			path = prefix + "." + f.Name
		}
		if st, ok := f.Type.(*arrow.StructType); ok {
			if err := flattenLeaves(st.Fields(), path, out); err != nil {
				return err
			}
			continue
		}
		li := &leafInfo{path: path, nullable: f.Nullable}
		switch t := f.Type.(type) {
		case *arrow.BooleanType:
			li.kind = kindBool
		case *arrow.FixedSizeBinaryType:
			li.kind, li.width = kindBytes, int64(t.ByteWidth)
		case *arrow.Decimal128Type:
			li.kind, li.width = kindBytes, 16
		case *arrow.Decimal256Type:
			li.kind, li.width = kindBytes, 32
		case *arrow.FixedSizeListType:
			class, size, ok := primShape(t.Elem())
			if !ok {
				return fmt.Errorf("ring: leaf %q: fixed_size_list element %s is not a fixed-width primitive", path, t.Elem())
			}
			li.kind, li.class, li.itemsize, li.width = kindFSL, class, size, int64(t.Len())
		case *arrow.StringType, *arrow.LargeStringType, *arrow.BinaryType, *arrow.LargeBinaryType,
			*arrow.ListType, *arrow.LargeListType:
			li.kind = kindVar
		default:
			class, size, ok := primShape(f.Type)
			if !ok {
				return fmt.Errorf("ring: leaf %q: type %s has no direct store", path, f.Type)
			}
			li.kind, li.class, li.itemsize = kindPrim, class, size
		}
		out[path] = li
	}
	return nil
}

func (r *Ring) widthErr(format string, a ...any) error {
	return &Error{Status: "width", Detail: fmt.Sprintf(format, a...)}
}

func (r *Ring) invalidErr(format string, a ...any) error {
	return &Error{Status: "invalid", Detail: fmt.Sprintf(format, a...)}
}

func bitSet(b unsafe.Pointer, i int64)   { p := (*byte)(unsafe.Add(b, i>>3)); *p |= 1 << (i & 7) }
func bitClear(b unsafe.Pointer, i int64) { p := (*byte)(unsafe.Add(b, i>>3)); *p &^= 1 << (i & 7) }

func (li *leafInfo) markValid(slot int64) {
	if li.valid != nil {
		bitSet(li.valid, slot)
	}
}

// storeInt mirrors the core's store_int: the same range refusals, then
// a little-endian store of itemsize bytes.
func (r *Ring) storeInt(li *leafInfo, slot int64, v int64, fromUnsigned bool, uv uint64) error {
	p := unsafe.Add(li.data, slot*li.itemsize)
	switch li.class {
	case classFloat:
		return r.invalidErr("field '%s': integer setter on float leaf", li.path)
	case classUnsigned:
		u := uv
		if !fromUnsigned {
			if v < 0 {
				return r.widthErr("field '%s': negative into unsigned", li.path)
			}
			u = uint64(v)
		}
		if li.itemsize < 8 && u>>(li.itemsize*8) != 0 {
			return r.widthErr("field '%s': %d overflows %d-byte leaf", li.path, u, li.itemsize)
		}
		switch li.itemsize {
		case 1:
			*(*uint8)(p) = uint8(u)
		case 2:
			*(*uint16)(p) = uint16(u)
		case 4:
			*(*uint32)(p) = uint32(u)
		default:
			*(*uint64)(p) = u
		}
		return nil
	}
	x := v
	if fromUnsigned {
		if uv > math.MaxInt64 {
			return r.widthErr("field '%s': u64 overflows signed leaf", li.path)
		}
		x = int64(uv)
	}
	if li.itemsize < 8 {
		lim := int64(1) << (li.itemsize*8 - 1)
		if x < -lim || x >= lim {
			return r.widthErr("field '%s': %d overflows %d-byte leaf", li.path, x, li.itemsize)
		}
	}
	switch li.itemsize {
	case 1:
		*(*int8)(p) = int8(x)
	case 2:
		*(*int16)(p) = int16(x)
	case 4:
		*(*int32)(p) = int32(x)
	default:
		*(*int64)(p) = x
	}
	return nil
}

func (r *Ring) leafInfo(leaf int32) (*leafInfo, error) {
	if leaf < 0 || int(leaf) >= len(r.info) {
		return nil, r.invalidErr("leaf index %d out of range", leaf)
	}
	return r.info[leaf], nil
}

// setF64ViaCore is the cgo path, kept only as the benchmark's baseline.
func (r *Ring) setF64ViaCore(leaf int32, v float64) error {
	if st := C.ar_set_f64(r.c, C.int32_t(leaf), C.double(v)); st != C.AR_OK {
		return r.err(st)
	}
	return nil
}
