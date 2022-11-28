package iondb

import (
	"testing"
	"unsafe"
)

func TestSkipListCombined(t *testing.T) {
	one := 1
	t.Run("skipList", func(t *testing.T) {
		dict := NewSkipList[int, int](-1, keyTypeNumericSigned, int(unsafe.Sizeof(one)), uint(unsafe.Sizeof(one)), 10)
		dict.Insert(3, 10)
		dict.Insert(4, 100)

		myVal := dict.Get(3)
		if dict.LastStatus.Err != ErrOk {
			t.Errorf("got err = %v, want = %v", dict.LastStatus.Err, ErrOk)
		}
		if myVal != 10 {
			t.Errorf("got val = %v, want = %v", myVal, 10)
		}

		myVal = dict.Get(4)
		if dict.LastStatus.Err != ErrOk {
			t.Errorf("got err = %v, want = %v", dict.LastStatus.Err, ErrOk)
		}
		if myVal != 100 {
			t.Errorf("got val = %v, want = %v", myVal, 100)
		}

		dict.DeleteRecord(4)
		myVal = dict.Get(4)
		if dict.LastStatus.Err != ErrItemNotFound {
			t.Errorf("got err = %v, want = %v", dict.LastStatus.Err, ErrItemNotFound)
		}
	})
}

func createTestDictionary(dict *IonDictionary, handler *IonDictionaryHandler, record *IonRecordInfo, kType IonKeyType, size int, numElements int) {
	SldictInit(handler)
	dictCreate(handler, dict, 1, kType, record.keySize, record.valueSize, IonDictionarySize(size))
	halfElem := numElements / 2
	val := "DATA"
	var i int
	numDuplicates := 0
	for i = 0; i < halfElem; i++ {
		dictInsert(dict, IonKey(&i), IonValue(&val))
	}
	for ; i < numElements; i++ {
		for j := 0; j < numDuplicates; j++ {
			dictInsert(dict, IonKey(&i), IonValue(&val))
		}
		numDuplicates++
	}
}

func createDictStdCond(dict *IonDictionary, handler *IonDictionaryHandler) {
	one := 1
	record := IonRecordInfo{IonKeySize(unsafe.Sizeof(one)), 10}
	kType := keyTypeNumericSigned
	size := 7
	numElements := 25
	createTestDictionary(dict, handler, &record, IonKeyType(kType), size, numElements)
}

func TestDictCreation(t *testing.T) {
	var dict IonDictionary
	var handler IonDictionaryHandler
	one := 1
	record := IonRecordInfo{IonKeySize(unsafe.Sizeof(one)), 10}
	kType := keyTypeNumericSigned
	size := 50
	numElements := 26

	t.Run("create dict", func(t *testing.T) {
		createTestDictionary(&dict, &handler, &record, IonKeyType(kType), size, numElements)
		skipList := (*ionSkipList)(unsafe.Pointer(dict.instance))
        printSkipList(skipList)
		if dict.instance.kType != keyTypeNumericSigned {
			t.Errorf("got keyType = %v, want = %v", dict.instance.kType, keyTypeNumericSigned)
		}
		if dict.instance.record.keySize != IonKeySize(unsafe.Sizeof(one)) {
			t.Errorf("got keySize = %v, want = %v", dict.instance.record.keySize, IonKeySize(unsafe.Sizeof(one)))
		}
		if dict.instance.record.valueSize != 10 {
			t.Errorf("got valueSize = %v, want = %v", dict.instance.record.valueSize, 10)
		}
		if skipList == nil {
			t.Errorf("got skipList = %v, want = %v", nil, "not nil")
		}
		if skipList.head == nil {
			t.Errorf("got skipList head = %v, want = %v", nil, "not nil")
		}
		if skipList.maxheight != 50 {
			t.Errorf("got skipList maxheight = %v, want = %v", skipList.maxheight, 50)
		}
		if skipList.pden != 4 {
			t.Errorf("got skipList pden = %v, want = %v", skipList.pden, 4)
		}
		if skipList.pnum != 1 {
			t.Errorf("got skipList pnum = %v, want = %v", skipList.pnum, 1)
		}
	})
}

func TestSkipListHandlerCursorEquality(t *testing.T) {
	var dict IonDictionary
	var handler IonDictionaryHandler
	createDictStdCond(&dict, &handler)

	var cursor *IonDictCursor
	predicate := new(IonPredicateEquality)
	predicate.equalityVal = IoNizeKey(33)

	t.Run("cursor", func(t *testing.T) {
		status := dictFind(&dict, predicate, &cursor)
		if status != ErrOk {
			t.Errorf("got equality = %v, want = %v", status, ErrOk)
		}
		if cursor.status != csEndOfResults {
			t.Errorf("got cursor status = %v, want = %v", cursor.status, csEndOfResults)
		}
	})
}

func TestSkipListHandlerCursorRangeWithResults(t *testing.T) {
	var dict IonDictionary
	var handler IonDictionaryHandler
	createDictStdCond(&dict, &handler)

	var cursor *IonDictCursor
	predicate := new(IonPredicateRange)
	predicate.lowerBound = IoNizeKey(5)
	predicate.upperBound = IoNizeKey(78)
	t.Run("cursor range", func(t *testing.T) {
		status := dictFind(&dict, predicate, &cursor)
		if status != ErrOk {
			t.Errorf("got range = %v, want = %v", status, ErrOk)
		}
		if cursor.status != csCursorInitialized {
			t.Errorf("got cursor status = %v, want = %v", cursor.status, csCursorInitialized)
		}

		var record IonRecord
		record.key = IonKey(alloc(uintptr(dict.instance.record.keySize), nil))
		record.value = IonValue(alloc(uintptr(dict.instance.record.valueSize), nil))

		cStatus := cursor.next(cursor, &record)
		if cStatus != csCursorActive {
			t.Errorf("got cursor status = %v, want = %v", cStatus, csCursorActive)
		}
		if *((*string)(record.value)) != "DATA" {
			t.Errorf("got cursor record = %v, want = %v", *((*string)(record.value)), "DATA")
		}
		for cStatus != csEndOfResults {
			if dict.instance.compare(record.key, IoNizeKey(5), dict.instance.record.keySize) < 0 {
				t.Errorf("got cursor lower key = %v, want = %v", *((*int)(record.key)), 5)
			}
			if dict.instance.compare(record.key, IoNizeKey(78), dict.instance.record.keySize) > 0 {
				t.Errorf("got cursor upper key = %v, want = %v", *((*int)(record.key)), 78)
			}
			if *((*string)(record.value)) != "DATA" {
				t.Errorf("got cursor record = %v, want = %v", *((*string)(record.value)), "DATA")
			}
			cStatus = cursor.next(cursor, &record)
		}
	})
}

func TestSkipListInit(t *testing.T) {
	type args struct {
		skipList  *ionSkipList
		kType     IonKeyType
		compare   IonDictionaryCompare
		maxheight ionSlLevel
		kSize     IonKeySize
		vSize     IonValueSize
		pnum      int
		pden      int
	}

	one := 1
	var skipList ionSkipList
	tests := []struct {
		name string
		args args
	}{
		{
			name: "init",
			args: args{
				skipList:  &skipList,
				kType:     keyTypeNumericSigned,
				compare:   dictCompareSignedValue,
				maxheight: ionSlLevel(7),
				kSize:     IonKeySize(unsafe.Sizeof(one)),
				vSize:     IonValueSize(10),
				pnum:      1,
				pden:      4,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			slInitialize(tt.args.skipList, tt.args.kType, tt.args.kSize, tt.args.vSize, tt.args.maxheight, tt.args.pnum, tt.args.pden)
			if tt.args.skipList.super.kType != keyTypeNumericSigned {
				t.Errorf("got keyType = %v, want = %v", tt.args.skipList.super.kType, tt.args.kType)
			}

			if tt.args.skipList.super.record.keySize != tt.args.kSize {
				t.Errorf("got keySize = %v, want = %v", tt.args.skipList.super.record.keySize, tt.args.kSize)
			}

			if tt.args.skipList.super.record.valueSize != tt.args.vSize {
				t.Errorf("got valSize = %v, want = %v", tt.args.skipList.super.record.valueSize, tt.args.vSize)
			}
		})
	}
}

func initSkipListStdCond(skipList *ionSkipList) {
	one := 1
	kType := IonKeyType(keyTypeNumericSigned)
	compare := dictCompareSignedValue
	maxheight := ionSlLevel(7)
	kSize := IonKeySize(unsafe.Sizeof(one))
	vSize := IonValueSize(10)
	pnum := 1
	pden := 4
	slInitialize(skipList, kType, kSize, vSize, maxheight, pnum, pden)
	skipList.super.compare = compare
}

func TestSkipListSingleInsert(t *testing.T) {
	var skipList ionSkipList
	initSkipListStdCond(&skipList)
	key := 6
	val := "single."
	tests := struct {
		key IonKey
		val IonValue
	}{
		key: IonKey(&key),
		val: IonValue(&val),
	}
	t.Run("single insert", func(t *testing.T) {
		status := slInsert(&skipList, tests.key, tests.val)
		if status.Err != ErrOk {
			t.Errorf("got err = %v, want = %v", status, ErrOk)
		}
		if status.ResCnt != 1 {
			t.Errorf("got resCnt = %v, want = %v", status.ResCnt, 1)
		}
		if *((*int)(skipList.head.next[0].key)) != key {
			t.Errorf("got head key = %v, want = %v", *((*int)(skipList.head.next[0].key)), key)
		}
		if *((*string)(skipList.head.next[0].val)) != val {
			t.Errorf("got head val = %v, want = %v", *((*string)(skipList.head.next[0].val)), val)
		}
	})
}

func TestSkipListMultipleInsert(t *testing.T) {
	var skipList ionSkipList
	initSkipListStdCond(&skipList)
	key := []int{1, 2, 3, 4, 5}
	val := []string{"one", "two", "three", "four", "five"}
	type test struct {
		key IonKey
		val IonValue
	}
	tests := []test{}
	for i := 0; i < 5; i++ {
		tests = append(tests, test{IonKey(&key[i]), IonValue(&val[i])})
	}

	t.Run("multiple insert", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			status := slInsert(&skipList, tests[i].key, tests[i].val)
			if status.Err != ErrOk {
				t.Errorf("got = %v, want = %v", status.Err, ErrOk)
			}
			if status.ResCnt != 1 {
				t.Errorf("got = %v, want = %v", status.ResCnt, 1)
			}
		}
		var cursor *ionSlNode
		cursor = skipList.head.next[0]
		if *((*int)(cursor.key)) != key[0] {
			t.Errorf("got head key = %v, want = %v", *((*int)(cursor.key)), key[0])
		}
		if *((*string)(cursor.val)) != val[0] {
			t.Errorf("got head val = %v, want = %v", *((*string)(cursor.val)), val[0])
		}

		cursor = skipList.head.next[0].next[0]
		if *((*int)(cursor.key)) != key[1] {
			t.Errorf("got head key = %v, want = %v", *((*int)(cursor.key)), key[1])
		}
		if *((*string)(cursor.val)) != val[1] {
			t.Errorf("got head val = %v, want = %v", *((*string)(cursor.val)), val[1])
		}

		cursor = skipList.head.next[0].next[0].next[0]
		if *((*int)(cursor.key)) != key[2] {
			t.Errorf("got head key = %v, want = %v", *((*int)(cursor.key)), key[2])
		}
		if *((*string)(cursor.val)) != val[2] {
			t.Errorf("got head val = %v, want = %v", *((*string)(cursor.val)), val[2])
		}

		cursor = skipList.head.next[0].next[0].next[0].next[0]
		if *((*int)(cursor.key)) != key[3] {
			t.Errorf("got head key = %v, want = %v", *((*int)(cursor.key)), key[3])
		}
		if *((*string)(cursor.val)) != val[3] {
			t.Errorf("got head val = %v, want = %v", *((*string)(cursor.val)), val[3])
		}

		cursor = skipList.head.next[0].next[0].next[0].next[0].next[0]
		if *((*int)(cursor.key)) != key[4] {
			t.Errorf("got head key = %v, want = %v", *((*int)(cursor.key)), key[4])
		}
		if *((*string)(cursor.val)) != val[4] {
			t.Errorf("got head val = %v, want = %v", *((*string)(cursor.val)), val[4])
		}
	})
}

func TestSkipListGetNodeSingle(t *testing.T) {
	var skipList ionSkipList
	initSkipListStdCond(&skipList)
	str := "find this"
	key := 3
	status := slInsert(&skipList, IonKey(&key), IonValue(&str))
	if status.Err != ErrOk {
		println("err: ", status.Err)
	}
	if status.ResCnt != 1 {
		println("resCntErr: ", status.ResCnt)
	}

	search := 3
	t.Run("get single", func(t *testing.T) {
		node := slFindNode(&skipList, IonKey(&search))
		if node == nil {
			t.Errorf("got node is nil, want is not nil")
		}
		if *((*int)(node.key)) != search {
			t.Errorf("got node key = %v, want = %v", *((*int)(node.key)), search)
		}
		if *((*string)(node.val)) != str {
			t.Errorf("got node val = %v, want = %v", *((*string)(node.val)), str)
		}
	})
}

func TestSkipListDeleteSingle(t *testing.T) {
	var skipList ionSkipList
	initSkipListStdCond(&skipList)
	key := 97
	str := "Special K"
	status := slInsert(&skipList, IonKey(&key), IonValue(&str))
	if status.Err != ErrOk {
		println("err: ", status.Err)
	}
	if status.ResCnt != 1 {
		println("resCntErr: ", status.ResCnt)
	}

	t.Run("delete single", func(t *testing.T) {
		status := slDelete(&skipList, IonKey(&key))
		if status.Err != ErrOk {
			t.Errorf("got err = %v, want = %v", status.Err, ErrOk)
		}
		if status.ResCnt != 1 {
			t.Errorf("got resCnt = %v, want = %v", status.ResCnt, 1)
		}
		if skipList.head.next[0] != nil {
			t.Errorf("got del node = %v, want = %v", skipList.head.next[0], nil)
		}
	})
}

var _ IonDictionaryHandler = slDictHandler{}
