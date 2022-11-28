package iondb

type Cursor[K, V any] struct {
	dict       *IonDictionary
	dictCursor *IonDictCursor
	record     IonRecord
}

func NewCursor[K, V any](dict *IonDictionary, predicate IonPredicate) *Cursor[K, V] {
	var cur Cursor[K, V]
	cur.dict = dict

	dictFind(dict, predicate, &cur.dictCursor)

	ionKeySlice := make([]IonByte, dict.instance.record.keySize)
	cur.record.key = IonKey(&ionKeySlice)
	ionValSlice := make([]IonByte, dict.instance.record.valueSize)
	cur.record.value = IonValue(&ionValSlice)
	return &cur
}

func (cursor *Cursor[K, V]) HasNext() bool {
	return cursor.dictCursor.status == csCursorInitialized || cursor.dictCursor.status == csCursorActive
}

func (cursor *Cursor[K, V]) Next() bool {
	status := cursor.dictCursor.next(cursor.dictCursor, &(cursor.record))
	return status == csCursorInitialized || status == csCursorActive
}

func (cursor *Cursor[K, V]) GetKey() K {
	return *((*K)(cursor.record.key))
}

func (cursor *Cursor[K, V]) GetValue() V {
	return *((*V)(cursor.record.value))
}
