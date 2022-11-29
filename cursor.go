package iondb

type Cursor[K, V any] struct {
	dict       *IonDictionary
	dictCursor *IonDictCursor
	record     IonRecord
}

func NewCursor[K, V any](dict *IonDictionary, predicate IonPredicate) *Cursor[K, V] {
	var cur Cursor[K, V]
	cur.dict = dict

	dictFind(dict, predicate, &(cur.dictCursor))

	cur.record.key = IonKey(alloc(uintptr(dict.instance.record.keySize), nil))
	cur.record.value = IonValue(alloc(uintptr(dict.instance.record.valueSize), nil))
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
