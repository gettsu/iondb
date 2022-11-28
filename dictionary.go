package iondb

import "unsafe"

type Dictionary[K, V any] interface {
	Insert(key K, val V) IonStatus
	Get(key K) V
	DeleteRecord(key K) IonStatus
	Update(key K, value V)
	DeleteDictionary() IonErr
	DestroyDIctionary(id IonDictionaryID) IonErr
	Open(confInfo IonDictionaryConfigInfo) IonErr
	Close() IonErr
	Range(minKey, maxKey K) *Cursor[K, V]
	Equality(key K) *Cursor[K, V]
	AllRecord() *Cursor[K, V]
}

type IonDictionary struct {
	status   IonDictionaryStatus
	instance *IonDictionaryParent
	handler  *IonDictionaryHandler
}

type IonDictionaryHandler interface {
	insert(dict *IonDictionary, key IonKey, val IonValue) IonStatus
	createDictionary(id IonDictionaryID, kType IonKeyType, kSize IonKeySize, vSize IonValueSize, dictSize IonDictionarySize, compare IonDictionaryCompare, handler *IonDictionaryHandler, dict *IonDictionary) IonErr
	get(dict *IonDictionary, key IonKey, val IonValue) IonStatus
	update(dict *IonDictionary, key IonKey, val IonValue) IonStatus
	find(dict *IonDictionary, predicate IonPredicate, cursor **IonDictCursor) IonErr
	remove(dict *IonDictionary, key IonKey) IonStatus
	deleteDictionary(dict *IonDictionary) IonErr
	destroyDictionary(id IonDictionaryID) IonErr
	openDictionary(handler *IonDictionaryHandler, dict *IonDictionary, conf *IonDictionaryConfigInfo, compare IonDictionaryCompare) IonErr
	closeDictionary(dict *IonDictionary) IonErr
}

// A type used to identify dictionaries, specifically in the master table.
type IonDictionaryID = int

// A type describing how a dictionary is used.
type IonDictionaryUse IonByte

// The position in the hashmap.
type IonHash int

func dictCreate(handler *IonDictionaryHandler, dict *IonDictionary, id IonDictionaryID, kType IonKeyType, kSize IonKeySize, vSize IonValueSize, dictSize IonDictionarySize) IonErr {
	compare := dictSwitchCompare(kType)
	err := (*handler).createDictionary(id, kType, kSize, vSize, dictSize, compare, handler, dict)
	if err == ErrOk {
		dict.instance.id = id
		dict.status = ionDictionaryStatusOk
	} else {
		dict.status = ionDictionaryStatusError
	}

	return err
}

func dictInsert(dict *IonDictionary, key IonKey, val IonValue) IonStatus {
	return (*(dict.handler)).insert(dict, key, val)
}

func dictGet(dict *IonDictionary, key IonKey, val IonValue) IonStatus {
	return (*(dict.handler)).get(dict, key, val)
}

func dictUpdate(dict *IonDictionary, key IonKey, val IonValue) IonStatus {
	return (*(dict.handler)).update(dict, key, val)
}

func dictDeleteDictionary(dict *IonDictionary) IonErr {
	return (*(dict.handler)).deleteDictionary(dict)
}

func dictDestroyDictionary(handler *IonDictionaryHandler, id IonDictionaryID) IonErr {
	err := (*handler).destroyDictionary(id)
	if err == ErrNotImplemented {
		// err = ffdictDestroyDictionary(id)
	}
	return err
}

func dictDelete(dict *IonDictionary, key IonKey) IonStatus {
	return (*(dict.handler)).remove(dict, key)
}

type IonDictionaryStatus int8

const (
	ionDictionaryStatusOk = iota
	ionDictionaryStatusClosed
	ionDictionaryStatusError
)

type IonDictionaryConfigInfo struct {
	id         IonDictionaryID
	useType    IonDictionaryUse
	kType      IonKeyType
	kSize      IonKeySize
	vSize      IonValueSize
	dictSize   IonDictionarySize
	dictType   IonDictionaryType
	dictStatus IonDictionaryStatus
}

type IonDictionaryParent struct {
	kType    IonKeyType
	record   IonRecordInfo
	compare  IonDictionaryCompare
	id       IonDictionaryID
	dictType IonDictionaryType
}

type IonDictionaryCompare func(firstKey IonKey, secondKey IonKey, keySize IonKeySize) int8

type IonDictCursor struct {
	status    IonCursorStatus
	dict      *IonDictionary
	predicate IonPredicate
	next      func(cursor *IonDictCursor, record *IonRecord) IonCursorStatus
	destroy   func(cursorPtr **IonDictCursor)
}

type IonCursorStatus int8

const (
	csInvalidIndex  = -1
	csInvalidCursor = iota
	csEndOfResults
	csCursorInitialized
	csCursorUninitialized
	csCursorActive
	csPossibleDataInconsistency
)

type IonPredicate interface {
	destroy()
}
type IonPredicateEquality struct {
	equalityVal IonKey
}

func (predicate *IonPredicateEquality) destroy() {
	predicate.equalityVal = nil
	predicate = nil
}

type IonPredicateRange struct {
	lowerBound IonKey
	upperBound IonKey
}

func (predicate *IonPredicateRange) destroy() {
	predicate.lowerBound = nil
	predicate.upperBound = nil
	predicate = nil
}

type IonPredicateAllRecords struct {
	unused int8
}

func (predicate *IonPredicateAllRecords) destroy() {
	predicate = nil
}

type IonPredicateStatement interface{}
type IonPredicateType int8

func dictSwitchCompare(kType IonKeyType) IonDictionaryCompare {
	var compare IonDictionaryCompare
	switch kType {
	case KeyTypeNumericSigned:
		compare = dictCompareSignedValue
	case KeyTypeNumericUnsigned:
		compare = dictCompareUnsignedValue
	case KeyTypeCharArray:
		compare = dictCompareCharArray
	case KeyTypeNullTerminatedString:
		compare = dictCompareNullTerminatedString
	}
	return compare
}

func dictCompareSignedValue(
	firstKey IonKey,
	secondKey IonKey,
	keySize IonKeySize,
) int8 {
	retVal := ionReturnValue
	idx := uintptr(0)
	firstByte := IonByte(*((*IonByte)(unsafe.Pointer(uintptr(firstKey) + idx))))
	secondByte := IonByte(*((*IonByte)(unsafe.Pointer(uintptr(secondKey) + idx))))
	// bigt comparison on sigend bit
	if retVal = int8((secondByte >> 7) - (firstByte >> 7)); retVal != 0 {
		return retVal
	}
	for ; IonKeySize(idx) < keySize; idx++ {
		firstByte = IonByte(*((*IonByte)(unsafe.Pointer(uintptr(firstKey) + idx))))
		secondByte = IonByte(*((*IonByte)(unsafe.Pointer(uintptr(secondKey) + idx))))
		if firstByte > secondByte {
			retVal = 1
			return retVal
		} else if firstByte < secondByte {
			retVal = -1
			return retVal
		}
	}
	return retVal
}

func dictCompareUnsignedValue(
	firstKey IonKey,
	secondKey IonKey,
	keySize IonKeySize,
) int8 {
	retVal := ionReturnValue
	// bigt comparison on sigend bit
	for idx := uintptr(0); IonKeySize(idx) < keySize; idx++ {
		firstByte := IonByte(*((*IonByte)(unsafe.Pointer(uintptr(firstKey) + idx))))
		secondByte := IonByte(*((*IonByte)(unsafe.Pointer(uintptr(secondKey) + idx))))
		if firstByte > secondByte {
			retVal = 1
			return retVal
		} else if firstByte < secondByte {
			retVal = -1
			return retVal
		}
	}
	return retVal
}

func dictCompareCharArray(
	firstKey IonKey,
	secondKey IonKey,
	keySize IonKeySize,
) int8 {
	for i := IonKeySize(0); i < keySize; i++ {
		fk := (*int8)(unsafe.Add(unsafe.Pointer(firstKey), i))
		sk := (*int8)(unsafe.Add(unsafe.Pointer(secondKey), i))
		if *fk > *sk {
			return 1
		} else if *fk < *sk {
			return -1
		}
	}
	return 0
}

func dictCompareNullTerminatedString(
	firstKey IonKey,
	secondKey IonKey,
	keySize IonKeySize,
) int8 {
	lhs := (*string)(firstKey)
	rhs := (*string)(secondKey)
	if *lhs > *rhs {
		return 1
	} else if *lhs < *rhs {
		return -1
	} else {
		return 0
	}
}

func dictOpen(
	handler *IonDictionaryHandler,
	dict *IonDictionary,
	conf *IonDictionaryConfigInfo,
) IonErr {
	compare := dictSwitchCompare(conf.kType)
	error := (*handler).openDictionary(handler, dict, conf, compare)

	if error == ErrNotImplemented {
		predicate := new(IonPredicateAllRecords)
		var cursor *IonDictCursor
		var record IonRecord
		var fallbackHandler IonDictionaryHandler
		var fallbackDict IonDictionary
		var err IonErr
		ffdictInit(&fallbackHandler)

		var fallbackConf IonDictionaryConfigInfo
		fallbackConf.id = conf.id
		fallbackConf.useType = 0
		fallbackConf.kType = conf.kType
		fallbackConf.kSize = conf.kSize
		fallbackConf.vSize = conf.vSize
		fallbackConf.dictSize = 1

		err = dictOpen(&fallbackHandler, &fallbackDict, &fallbackConf)
		if err != ErrOk {
			return err
		}
		err = dictFind(&fallbackDict, predicate, &cursor)

		if err != ErrOk {
			return err
		}
		ionKeySlice := make([]IonByte, conf.kSize)
		record.key = IonKey(&ionKeySlice)
		ionValSlice := make([]IonByte, conf.vSize)
		record.value = IonValue(&ionValSlice)

		err = dictCreate(handler, dict, conf.id, conf.kType, conf.kSize, conf.vSize, conf.dictSize)
		if err != ErrOk {
			return err
		}
		cursorStatus := cursor.next(cursor, &record)
		for ; cursorStatus == csCursorActive || cursorStatus == csCursorInitialized; cursorStatus = cursor.next(cursor, &record) {
			status := dictInsert(dict, record.key, record.value)

			if status.Err != ErrOk {
				cursor.destroy(&cursor)
				dictClose(&fallbackDict)
				dictDeleteDictionary(dict)
				return status.Err
			}
		}

		if cursorStatus != csEndOfResults {
			return ErrUninitialized
		}

		err = dictDeleteDictionary(&fallbackDict)

		if err != ErrOk {
			return err
		}

		error = ErrOk
	}

	if error == ErrOk {
		dict.status = ionDictionaryStatusOk
		dict.instance.id = conf.id
	} else {
		dict.status = ionDictionaryStatusError
	}

	return error
}

func dictClose(dict *IonDictionary) IonErr {
	if dict.status == ionDictionaryStatusClosed {
		return ErrOk
	}

	error := (*(dict.handler)).closeDictionary(dict)

	if error == ErrNotImplemented {
		predicate := new(IonPredicateAllRecords)
		var cursor *IonDictCursor
		var record IonRecord
		var err IonErr

		err = dictFind(dict, predicate, &cursor)

		if err != ErrOk {
			return err
		}

		kSize := dict.instance.record.keySize
		vSize := dict.instance.record.valueSize
		kType := dict.instance.kType

		ionKeySlice := make([]IonByte, kSize)
		record.key = IonKey(&ionKeySlice)
		ionValSlice := make([]IonByte, vSize)
		record.value = IonValue(&ionValSlice)

		var fallbackHandler IonDictionaryHandler
		var fallbackDict IonDictionary

		err = dictCreate(&fallbackHandler, &fallbackDict, dict.instance.id, kType, kSize, vSize, 1)
		if err != ErrOk {
			return err
		}
		cursorStatus := cursor.next(cursor, &record)
		for ; cursorStatus == csCursorActive || cursorStatus == csCursorInitialized; cursorStatus = cursor.next(cursor, &record) {
			status := dictInsert(&fallbackDict, record.key, record.value)

			if status.Err != ErrOk {
				cursor.destroy(&cursor)
				dictDeleteDictionary(&fallbackDict)
				return status.Err
			}
		}

		if cursorStatus != csEndOfResults && cursorStatus != csCursorUninitialized {
			return ErrUninitialized
		}

		cursor.destroy(&cursor)

		err = dictClose(&fallbackDict)

		if err != ErrOk {
			return err
		}

		err = dictDeleteDictionary(dict)

		if err != ErrOk {
			return err
		}
		error = ErrOk
	}

	if error == ErrOk {
		dict.status = ionDictionaryStatusClosed
	}

	return error
}

func dictFind(dict *IonDictionary, predicate IonPredicate, cursor **IonDictCursor) IonErr {
	return (*(dict.handler)).find(dict, predicate, cursor)
}

func testPredicate(cursor *IonDictCursor, key IonKey) bool {
	parent := cursor.dict.instance
	kSize := cursor.dict.instance.record.keySize
	switch v := cursor.predicate.(type) {
	case *IonPredicateEquality:
		if parent.compare(key, v.equalityVal, kSize) == 0 {
			return true
		}
	case *IonPredicateRange:
		lowerBound := v.lowerBound
		upperBound := v.upperBound
		compLower := parent.compare(key, lowerBound, kSize) >= 0
		compUpper := parent.compare(key, upperBound, kSize) <= 0
		return compLower && compUpper
	case *IonPredicateAllRecords:
		return true
	}
	return false
}

// tinygoにはmemcpyが組み込みで存在しないので、tinygoのラインタイムが実装している
// memcpyを無理やり利用する
//
//go:linkname memcpy runtime.memcpy
func memcpy(dst, src unsafe.Pointer, size uintptr)

//go:linkname alloc runtime.alloc
func alloc(size uintptr, layout unsafe.Pointer) unsafe.Pointer
