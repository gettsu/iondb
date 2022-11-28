package iondb

import (
	"testing"
	"unsafe"
)

func TestCompareSigned(t *testing.T) {
	type args struct {
		key1 IonKey
		key2 IonKey
	}
	minus := -1
	one := 1
	one2 := 1
	two := 2
	eighty := 80
	tests := []struct {
		name string
		args args
		want int8
	}{
		{
			name: "signed eq",
			args: args{key1: IonKey(&one), key2: IonKey(&one2)},
			want: 0,
		},
		{
			name: "signed lt",
			args: args{key1: IonKey(&one), key2: IonKey(&two)},
			want: -1,
		},
		{
			name: "signed gt",
			args: args{key1: IonKey(&two), key2: IonKey(&minus)},
			want: 1,
		},
		{
			name: "signed much lt",
			args: args{key1: IonKey(&two), key2: IonKey(&eighty)},
			want: -1,
		},
		{
			name: "signed much gt",
			args: args{key1: IonKey(&eighty), key2: IonKey(&minus)},
			want: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := dictCompareSignedValue(tt.args.key1, tt.args.key2, IonKeySize(unsafe.Sizeof(int(1)))); got != tt.want {
				t.Errorf("dictCompareSignedValue() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestCompareUnsigned(t *testing.T) {
	type args struct {
		key1 IonKey
		key2 IonKey
	}
	uone := uint(1)
	utwo := uint(2)
	tests := []struct {
		name string
		args args
		want int8
	}{
		{
			name: "unsigned gt",
			args: args{key1: IonKey(&uone), key2: IonKey(&utwo)},
			want: -1,
		},
		{
			name: "unsigned lt",
			args: args{key1: IonKey(&utwo), key2: IonKey(&uone)},
			want: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := dictCompareUnsignedValue(tt.args.key1, tt.args.key2, IonKeySize(unsafe.Sizeof(int(1)))); got != tt.want {
				t.Errorf("dictCompareUnsignedValue() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestCompareCharArray(t *testing.T) {
	type args struct {
		key1 IonKey
		key2 IonKey
	}
	type obj struct {
		num1 int
		num2 int
		str  string
	}
	ex1 := obj{1, 1, "hi"}
	ex2 := obj{2, 1, "hi"}
	ex3 := obj{1, 2, "hi"}
	bytes1 := make([]int8, unsafe.Sizeof(ex1))
	bytes2 := make([]int8, unsafe.Sizeof(ex2))
	bytes3 := make([]int8, unsafe.Sizeof(ex3))
	memcpy(unsafe.Pointer(&bytes1[0]), unsafe.Pointer(&ex1), unsafe.Sizeof(ex1))
	memcpy(unsafe.Pointer(&bytes2[0]), unsafe.Pointer(&ex2), unsafe.Sizeof(ex2))
	memcpy(unsafe.Pointer(&bytes3[0]), unsafe.Pointer(&ex3), unsafe.Sizeof(ex3))
	tests := []struct {
		name string
		args args
		want int8
	}{
		{
			name: "charArray lt",
			args: args{key1: IonKey(&bytes1[0]), key2: IonKey(&bytes2[0])},
			want: -1,
		},
		{
			name: "charArray eq",
			args: args{key1: IonKey(&bytes1[0]), key2: IonKey(&bytes1[0])},
			want: 0,
		},
		{
			name: "charArray gt",
			args: args{key1: IonKey(&bytes2[0]), key2: IonKey(&bytes1[0])},
			want: 1,
		},
		{
			name: "charArray a bit gt",
			args: args{key1: IonKey(&bytes3[0]), key2: IonKey(&bytes1[0])},
			want: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := dictCompareCharArray(tt.args.key1, tt.args.key2, IonKeySize(unsafe.Sizeof(ex1))); got != tt.want {
				t.Errorf("dictCompareCharArrayValue() = %v, want = %v", got, tt.want)
			}
		})
	}
}

func TestCompareString(t *testing.T) {
	type args struct {
		key1 IonKey
		key2 IonKey
	}
	hi := "hi"
	yes := "yes"
	yez := "yez"
	tests := []struct {
		name string
		args args
		want int8
	}{
		{
			name: "string lt",
			args: args{key1: IonKey(&hi), key2: IonKey(&yes)},
			want: -1,
		},
		{
			name: "string eq",
			args: args{key1: IonKey(&hi), key2: IonKey(&hi)},
			want: 0,
		},
		{
			name: "string gt",
			args: args{key1: IonKey(&yez), key2: IonKey(&yes)},
			want: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := dictCompareNullTerminatedString(tt.args.key1, tt.args.key2, IonKeySize(2)); got != tt.want {
				t.Errorf("dictCompareStringValue() = %v, want = %v", got, tt.want)
			}
		})
	}
}
