package db

import "fmt"

const (
	Int = iota
	Int8
	Int16
	Int32
	Int64
	UInt
	UInt8
	UInt16
	UInt32
	UInt64
	Numeric
	Float32
	Float64
	Bool
	String
	Date
	DateTime
	IPAddr

	MapStringToString
	// TODO: How can we deal with other kinds of Maps?
	// MapStringToInt
	// MapStringToInt8
	// MapStringToInt16
	// MapStringToInt32
	// MapStringToInt64
	// MapStringToUInt
	// MapStringToUInt8
	// MapStringToUInt16
	// MapStringToUInt32
	// MapStringToUInt64
	// MapStringToNumeric
	// MapStringToFloat32
	// MapStringToFloat64
	// MapStringToBool
	// MapStringToDate
	// MapStringToDateTime

	IntArray
	Int8Array
	Int16Array
	Int32Array
	Int64Array
	UIntArray
	UInt8Array
	UInt16Array
	UInt32Array
	UInt64Array
	NumericArray
	Float32Array
	Float64Array
	BoolArray
	StringArray
	DateArray
	DateTimeArray
	IPAddrArray
	MapStringToStringArray
)

func TypeStr(t uint8) string {
	switch t {
	case Int:
		return "Int"
	case Int8:
		return "Int8"
	case Int16:
		return "Int16"
	case Int32:
		return "Int32"
	case Int64:
		return "Int64"
	case UInt:
		return "UInt"
	case UInt8:
		return "UInt8"
	case UInt16:
		return "UInt16"
	case UInt32:
		return "UInt32"
	case UInt64:
		return "UInt64"
	case Numeric:
		return "Numeric"
	case Float32:
		return "Float32"
	case Float64:
		return "Float64"
	case Bool:
		return "Bool"
	case String:
		return "String"
	case Date:
		return "Date"
	case DateTime:
		return "DateTime"
	case IPAddr:
		return "IPAddr"
	case MapStringToString:
		return "MapStringToString"
	case IntArray:
		return "IntArray"
	case Int8Array:
		return "Int8Array"
	case Int16Array:
		return "Int16Array"
	case Int32Array:
		return "Int32Array"
	case Int64Array:
		return "Int64Array"
	case UIntArray:
		return "UIntArray"
	case UInt8Array:
		return "UInt8Array"
	case UInt16Array:
		return "UInt16Array"
	case UInt32Array:
		return "UInt32Array"
	case UInt64Array:
		return "UInt64Array"
	case NumericArray:
		return "NumericArray"
	case Float32Array:
		return "Float32Array"
	case Float64Array:
		return "Float64Array"
	case BoolArray:
		return "BoolArray"
	case StringArray:
		return "StringArray"
	case DateArray:
		return "DateArray"
	case DateTimeArray:
		return "DateTimeArray"
	case IPAddrArray:
		return "IPAddrArray"
	case MapStringToStringArray:
		return "MapStringToStringArray"
	default:
		panic(fmt.Sprintf("Unknown type %d\n", t))
	}
}
