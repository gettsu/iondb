package iondb

import "unsafe"

type IonKeyType int

const (
	KeyTypeNumericSigned = iota
	KeyTypeNumericUnsigned
	KeyTypeCharArray
	KeyTypeNullTerminatedString
)

type IonDictionaryType int

const (
	DictionaryTypeBppTree = iota
	DIctionaryTypeFlatFile
	DictionaryTypeOpenAddressFileHash
	DictionaryTypeOpenAddressHash
	DictionaryTypeSkipList
	DictionaryTypeLinearHash
)

type IonByte uint8

type IonErr int8

const (
	ErrOk = iota
	ErrItemNotFound
	ErrDuplicateKey
	ErrMaxCapacity
	ErrDictionaryDestructionError
	ErrInvalidPredicate
	ErrOutOfMemory
	ErrFileWriteError
	ErrFileReadError
	ErrFileOpenError
	ErrFileCloseError
	ErrFileDeleteError
	ErrUnableToConvert
	ErrUnableToInsert
	ErrFileBadSeek
	ErrFileHitEof
	ErrNotImplemented
	ErrInvalidiInitialSize
	ErrDuplicateDictionaryError
	ErrUninitialized
	ErrOutOfBounds
	ErrSortedOrderViolation
)

type IonKey unsafe.Pointer
type IonValue unsafe.Pointer
type IonKeySize = int
type IonValueSize = uint
type IonDictionarySize uint

type IonResultCount int
type IonStatus struct {
	Err    IonErr
	ResCnt IonResultCount
}

type IonRecordInfo struct {
	keySize   IonKeySize
	valueSize IonValueSize
}

type IonRecord struct {
	key   IonKey
	value IonValue
}

const (
	ionReturnValue int8 = 0x73
)

func IoNizeKey[A any](item A) IonKey {
	return IonKey(unsafe.Pointer(&item))
}
func IoNizeValue[A any](item A) IonKey {
	return IonKey(unsafe.Pointer(&item))
}
