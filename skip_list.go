package iondb

import (
	"math/rand"
	"unsafe"
)

const slDebug = false

type SkipList[K, V any] struct {
	handler    IonDictionaryHandler
	dict       IonDictionary
	keyType    IonKeyType
	keySize    IonKeySize
	valSize    IonValueSize
	dictSize   IonDictionarySize
	LastStatus IonStatus
}

func NewSkipList[K, V any](id IonDictionaryID, kType IonKeyType, kSize IonKeySize, vSize IonValueSize, dictSize IonDictionarySize) *SkipList[K, V] {
	sl := new(SkipList[K, V])
	SldictInit(&(sl.handler))

	sl.keyType = kType
	sl.keySize = kSize
	sl.valSize = vSize
	sl.dictSize = dictSize

	err := dictCreate(&(sl.handler), &(sl.dict), id, kType, kSize, vSize, dictSize)

	sl.LastStatus.Err = err
	return sl
}

func (sl *SkipList[K, V]) Insert(key K, val V) IonStatus {
	ionKey := (IonKey)(unsafe.Pointer(&key))
	ionVal := (IonValue)(unsafe.Pointer(&val))
	status := dictInsert(&(sl.dict), ionKey, ionVal)
	sl.LastStatus = status
	return status
}

func (sl *SkipList[K, V]) Get(key K) V {
	ionKey := (IonKey)(unsafe.Pointer(&key))
	ionValSlice := make([]IonByte, sl.dict.instance.record.valueSize)
	ionVal := (IonValue)(unsafe.Pointer(&ionValSlice[0]))
	status := dictGet(&(sl.dict), ionKey, ionVal)
	sl.LastStatus = status
	return *((*V)(ionVal))
}

func (sl *SkipList[K, V]) DeleteRecord(key K) IonStatus {
	ionKey := (IonKey)(unsafe.Pointer(&key))
	status := dictDelete(&(sl.dict), ionKey)
	sl.LastStatus = status
	return status
}

func (sl *SkipList[K, V]) Update(key K, val V) IonStatus {
	ionKey := (IonKey)(unsafe.Pointer(&key))
	ionVal := (IonValue)(unsafe.Pointer(&val))
	status := dictUpdate(&(sl.dict), ionKey, ionVal)
	sl.LastStatus = status
	return status
}

func (sl *SkipList[K, V]) DeleteDictionary() IonErr {
	err := dictDeleteDictionary(&(sl.dict))
	sl.LastStatus.Err = err
	return err
}

func (sl *SkipList[K, V]) DestroyDictionary(id IonDictionaryID) IonErr {
	err := dictDestroyDictionary(&(sl.handler), id)
	sl.LastStatus.Err = err
	return err
}

func (sl *SkipList[K, V]) Open(configInfo IonDictionaryConfigInfo) IonErr {
	err := dictOpen(&(sl.handler), &(sl.dict), &configInfo)
	sl.keyType = configInfo.kType
	sl.keySize = configInfo.kSize
	sl.valSize = configInfo.vSize
	sl.dictSize = configInfo.dictSize
	sl.LastStatus.Err = err
	return err
}

func (sl *SkipList[K, V]) Close() IonErr {
	err := dictClose(&(sl.dict))
	sl.LastStatus.Err = err
	return err
}

func (sl *SkipList[K, V]) Range(minKey, maxKey K) *Cursor[K, V] {
	predicate := new(IonPredicateRange)
	ionMinKey := IonKey(unsafe.Pointer(&minKey))
	ionMaxKey := IonKey(unsafe.Pointer(&maxKey))

	predicate.lowerBound = ionMinKey
	predicate.upperBound = ionMaxKey
	return NewCursor[K, V](&(sl.dict), predicate)
}

func (sl *SkipList[K, V]) Equality(key K) *Cursor[K, V] {
	predicate := new(IonPredicateEquality)
	ionKey := IonKey(unsafe.Pointer(&key))

	predicate.equalityVal = ionKey
	return NewCursor[K, V](&(sl.dict), predicate)
}

func (sl *SkipList[K, V]) AllRecords() *Cursor[K, V] {
	predicate := new(IonPredicateAllRecords)
	return NewCursor[K, V](&(sl.dict), predicate)
}

type slDictHandler struct{}

func SldictInit(handler *IonDictionaryHandler) {
	var dictHandler slDictHandler
	*handler = dictHandler
}

func (slHandler slDictHandler) insert(dict *IonDictionary, key IonKey, val IonValue) IonStatus {
	return slInsert((*ionSkipList)(unsafe.Pointer(dict.instance)), key, val)
}

func (slHandler slDictHandler) createDictionary(id IonDictionaryID, kType IonKeyType, kSize IonKeySize, vSize IonValueSize, dictSize IonDictionarySize, compare IonDictionaryCompare, handler *IonDictionaryHandler, dict *IonDictionary) IonErr {
	_ = id
	var skipList ionSkipList
	dict.instance = (*IonDictionaryParent)(unsafe.Pointer(&skipList))

	dict.instance.compare = compare
	dict.instance.kType = DictionaryTypeSkipList

	pnum := 1
	pden := 4
	ret := slInitialize((*ionSkipList)(unsafe.Pointer(dict.instance)), kType, kSize, vSize, ionSlLevel(dictSize), pnum, pden)

	if ret == ErrOk && handler != nil {
		dict.handler = handler
	}
	return ret
}

func (slHandler slDictHandler) get(dict *IonDictionary, key IonKey, val IonValue) IonStatus {
	return slGet((*ionSkipList)(unsafe.Pointer(dict.instance)), key, val)
}
func (slHandler slDictHandler) update(dict *IonDictionary, key IonKey, val IonValue) IonStatus {
	return slUpdate((*ionSkipList)(unsafe.Pointer(dict.instance)), key, val)
}

func slDictDestroyCursor(cursor **IonDictCursor) {
	(*cursor).predicate.destroy()
	*cursor = nil
}

func slDictNext(cursor *IonDictCursor, record *IonRecord) IonCursorStatus {
	slCursor := (*ionSlDictCursor)(unsafe.Pointer(cursor))
	if cursor.status == csCursorUninitialized {
		return cursor.status
	} else if cursor.status == csEndOfResults {
		return cursor.status
	} else if cursor.status == csCursorInitialized || cursor.status == csCursorActive {
		if cursor.status == csCursorActive {
			if slCursor.current == nil || testPredicate(cursor, slCursor.current.key) == false {
				cursor.status = csEndOfResults
				return cursor.status
			}
		} else {
			cursor.status = csCursorActive
		}
		memcpy(unsafe.Pointer(record.key), unsafe.Pointer(slCursor.current.key), uintptr(cursor.dict.instance.record.keySize))
		memcpy(unsafe.Pointer(record.value), unsafe.Pointer(slCursor.current.val), uintptr(cursor.dict.instance.record.valueSize))

		slCursor.current = slCursor.current.next[0]
		return cursor.status
	}

	return csInvalidCursor
}

func (slHandler slDictHandler) find(dict *IonDictionary, predicate IonPredicate, cursor **IonDictCursor) IonErr {
	*cursor = (*IonDictCursor)(unsafe.Pointer(new(ionSlDictCursor)))
	(*cursor).dict = dict
	(*cursor).status = csCursorUninitialized
	(*cursor).destroy = slDictDestroyCursor
	(*cursor).next = slDictNext
	switch v := predicate.(type) {
	case *IonPredicateEquality:
		newPredicate := new(IonPredicateEquality)
		targetKey := v.equalityVal
		kSize := dict.instance.record.keySize
		newPredicate.equalityVal = IonKey(alloc(uintptr(kSize), nil))
		memcpy(unsafe.Pointer(newPredicate.equalityVal), unsafe.Pointer(targetKey), uintptr(kSize))
		(*cursor).predicate = newPredicate

		loc := slFindNode((*ionSkipList)(unsafe.Pointer(dict.instance)), targetKey)
		if loc.key == nil || dict.instance.compare(loc.key, targetKey, kSize) != 0 {
			(*cursor).status = csEndOfResults
			return ErrOk
		} else {
			(*cursor).status = csCursorInitialized

			slCursor := (*ionSlDictCursor)(unsafe.Pointer(*cursor))
			slCursor.current = loc
			return ErrOk
		}
	case *IonPredicateRange:
		kSize := dict.instance.record.keySize
		newPredicate := new(IonPredicateRange)
		newPredicate.lowerBound = IonKey(alloc(uintptr(kSize), nil))
		memcpy(unsafe.Pointer(newPredicate.lowerBound), unsafe.Pointer(v.lowerBound), uintptr(kSize))
		newPredicate.upperBound = IonKey(alloc(uintptr(kSize), nil))
		memcpy(unsafe.Pointer(newPredicate.upperBound), unsafe.Pointer(v.upperBound), uintptr(kSize))

		(*cursor).predicate = newPredicate
		loc := slFindNode((*ionSkipList)(unsafe.Pointer(dict.instance)), v.upperBound)
		if loc.key == nil || dict.instance.compare(loc.key, newPredicate.lowerBound, kSize) < 0 {
			(*cursor).status = csEndOfResults
			return ErrOk
		} else {
			loc = slFindNode((*ionSkipList)(unsafe.Pointer(dict.instance)), v.lowerBound)
			if loc.key == nil {
				loc = loc.next[0]
			}

			for loc != nil && (dict.instance.compare(loc.key, v.lowerBound, kSize) < 0) {
				loc = loc.next[0]
			}

			if loc == nil {
				(*cursor).status = csEndOfResults
				return ErrOk
			}

			(*cursor).status = csCursorInitialized

			slCursor := (*ionSlDictCursor)(unsafe.Pointer(*cursor))
			slCursor.current = loc
			return ErrOk
		}
	case *IonPredicateAllRecords:
		slCursor := (*ionSlDictCursor)(unsafe.Pointer(*cursor))
		skipList := (*ionSkipList)(unsafe.Pointer(dict.instance))
		if skipList.head.next[0] == nil {
			(*cursor).status = csEndOfResults
		} else {
			slCursor.current = skipList.head.next[0]
			(*cursor).status = csCursorInitialized
		}
		return ErrOk
	default:
		return ErrInvalidPredicate
	}
}
func (slHandler slDictHandler) remove(dict *IonDictionary, key IonKey) IonStatus {
	return slDelete((*ionSkipList)(unsafe.Pointer(dict.instance)), key)
}

func (slHandler slDictHandler) deleteDictionary(dict *IonDictionary) IonErr {
	ret := slDestroy((*ionSkipList)(unsafe.Pointer(dict.instance)))
	dict.instance = nil
	return ret
}

func (slHandler slDictHandler) destroyDictionary(id IonDictionaryID) IonErr {
	_ = id
	return ErrNotImplemented
}

func (slHandler slDictHandler) openDictionary(handler *IonDictionaryHandler, dict *IonDictionary, conf *IonDictionaryConfigInfo, compare IonDictionaryCompare) IonErr {
	return ErrNotImplemented
}

func (slHandler slDictHandler) closeDictionary(dict *IonDictionary) IonErr {
	return ErrNotImplemented
}

type ionSkipList struct {
	super     IonDictionaryParent
	head      *ionSlNode
	maxheight ionSlLevel
	pnum      int
	pden      int
}

type ionSlLevel int

type ionSlNode struct {
	key    IonKey
	val    IonValue
	height ionSlLevel
	next   []*ionSlNode
}

type ionSlDictCursor struct {
	super   IonDictCursor
	current *ionSlNode
}

func slInitialize(skipList *ionSkipList, kType IonKeyType, kSize IonKeySize, vSize IonValueSize, maxheight ionSlLevel, pnum int, pden int) IonErr {
	skipList.super.kType = kType
	skipList.super.record.keySize = kSize
	skipList.super.record.valueSize = vSize
	skipList.maxheight = maxheight
	skipList.pnum = pnum
	skipList.pden = pden

	if slDebug {
		println("skipList super record kSize :", kSize)
		println("skipList super record vSize :", vSize)
		println("skipList maxheight :", maxheight)
		println("skipList pnum :", pnum)
		println("skipList pden :", pden)
	}

	skipList.head = new(ionSlNode)

	tmpArray := make([]*ionSlNode, skipList.maxheight)
	skipList.head.next = tmpArray

	skipList.head.height = maxheight - 1
	skipList.head.key = nil
	skipList.head.val = nil

	maxheight--
	for ; maxheight > 0; maxheight-- {
		skipList.head.next[maxheight] = nil
	}
	return ErrOk
}

func slInsert(skipList *ionSkipList, key IonKey, val IonValue) IonStatus {
	kSize := skipList.super.record.keySize
	vSize := skipList.super.record.valueSize

	newNode := new(ionSlNode)
	newNode.key = IonKey(alloc(uintptr(kSize), nil))
	newNode.val = IonValue(alloc(uintptr(vSize), nil))
	memcpy(unsafe.Pointer(newNode.key), unsafe.Pointer(key), uintptr(kSize))
	memcpy(unsafe.Pointer(newNode.val), unsafe.Pointer(val), uintptr(vSize))

	duplicate := slFindNode(skipList, key)
	if duplicate.key != nil && skipList.super.compare(duplicate.key, key, kSize) == 0 {
		newNode.height = 0
		newNode.next = make([]*ionSlNode, newNode.height+1)
		for duplicate.next[0] != nil && skipList.super.compare(duplicate.next[0].key, key, kSize) == 0 {
			duplicate = duplicate.next[0]
		}
		newNode.next[0] = duplicate.next[0]
		duplicate.next[0] = newNode
	} else {
		newNode.height = slGenLevel(skipList)

		newNode.next = make([]*ionSlNode, newNode.height+1)

		cursor := skipList.head
		var h ionSlLevel
		for h = skipList.head.height; h >= 0; h-- {
			for cursor.next[h] != nil && skipList.super.compare(key, cursor.next[h].key, kSize) >= 0 {
				cursor = cursor.next[h]
			}

			if h <= newNode.height {
				newNode.next[h] = cursor.next[h]
				cursor.next[h] = newNode
			}
		}
	}
	return IonStatus{ErrOk, 1}
}

func slDestroy(skipList *ionSkipList) IonErr {
	cursor := skipList.head
	var toFree *ionSlNode
	for cursor != nil {
		toFree = cursor
		cursor = cursor.next[0]
		toFree.key = nil
		toFree.val = nil
		toFree.next = nil
		toFree = nil
	}

	skipList.head = nil
	return ErrOk
}

func slGet(skipList *ionSkipList, key IonKey, val IonValue) IonStatus {
	kSize := skipList.super.record.keySize
	vSize := skipList.super.record.valueSize
	cursor := slFindNode(skipList, key)
	if (cursor.key == nil) || (skipList.super.compare(cursor.key, key, kSize) != 0) {
		return IonStatus{ErrItemNotFound, 0}
	}

	memcpy(unsafe.Pointer(val), unsafe.Pointer(cursor.val), uintptr(vSize))
	return IonStatus{ErrOk, 1}
}

func slUpdate(skipList *ionSkipList, key IonKey, val IonValue) IonStatus {
	status := IonStatus{ErrUninitialized, 0}
	kSize := skipList.super.record.keySize
	vSize := skipList.super.record.valueSize
	cursor := slFindNode(skipList, key)
	if (cursor.key == nil) || (skipList.super.compare(cursor.key, key, kSize) != 0) {
		status.Err = slInsert(skipList, key, val).Err
		status.ResCnt = 1
		return status
	}
	for cursor != nil && skipList.super.compare(cursor.key, key, skipList.super.record.keySize) == 0 {
		memcpy(unsafe.Pointer(cursor.val), unsafe.Pointer(val), uintptr(vSize))
		cursor = cursor.next[0]
		status.ResCnt++
	}
	status.Err = ErrOk
	return status
}

func slDelete(skipList *ionSkipList, key IonKey) IonStatus {
	kSize := skipList.super.record.keySize
	status := IonStatus{ErrItemNotFound, 0}
	cursor := skipList.head
	var h ionSlLevel
	for h = skipList.head.height; h >= 0; h-- {
		for cursor.next[h] != nil && skipList.super.compare(cursor.next[h].key, key, kSize) < 0 {
			cursor = cursor.next[h]
		}
		if (cursor.next[h] != nil) && (skipList.super.compare(cursor.next[h].key, key, kSize) == 0) {
			oldCursor := cursor
			for cursor.next[h] != nil && skipList.super.compare(cursor.next[h].key, key, kSize) == 0 {
				toFree := cursor.next[h]
				reLink := cursor.next[h]
				linkH := reLink.height

				for linkH >= 0 {
					for cursor.next[linkH] != reLink {
						cursor = cursor.next[linkH]
					}
					jump := reLink.next[linkH]
					cursor.next[linkH] = jump
					linkH--
				}
				toFree.key = nil
				toFree.val = nil
				toFree.next = nil
				toFree = nil

				cursor = oldCursor
				status.ResCnt++
			}
			status.Err = ErrOk
		}
	}
	return status
}

func slFindNode(skipList *ionSkipList, key IonKey) *ionSlNode {
	kSize := skipList.super.record.keySize
	cursor := skipList.head
	var h ionSlLevel
	for h = skipList.head.height; h >= 0; h-- {
		for cursor.next[h] != nil && skipList.super.compare(cursor.next[h].key, key, kSize) <= 0 {
			if cursor.next[h] != nil && skipList.super.compare(cursor.next[h].key, key, kSize) == 0 {
				return cursor.next[h]
			}
			cursor = cursor.next[h]
		}
	}
	return cursor
}

func slGenLevel(skipList *ionSkipList) ionSlLevel {
	level := ionSlLevel(1)
	for rand.Float32() < float32(skipList.pnum)/float32(skipList.pden) && level < skipList.maxheight {
		level++
	}
	return level - 1
}

func printSkipList[V any](skipList *ionSkipList) {
	cursor := skipList.head
	for cursor.next[0] != nil {
		level := cursor.next[0].height + 1

		if skipList.super.kType == KeyTypeNumericSigned {
			key := *((*int)(cursor.next[0].key))
			val := *((*V)(cursor.next[0].val))

			println("k: ", key, "(v: ", val, ") [l: ", level, "]")
		} else if skipList.super.kType == KeyTypeNullTerminatedString {
			key := *((*string)(cursor.next[0].key))
			val := *((*V)(cursor.next[0].val))
			println("k: ", key, "(v: ", val, ") [l: ", level, "]")
		}
		cursor = cursor.next[0]
	}
	println("end")
}
