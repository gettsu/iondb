# IonDB

A port of [IonDB](https://github.com/iondbproject/iondb) for TinyGo.

usage:
```go
package main

import "iondb"

func main() {

    dict := iondb.NewSkipList[int, int](-1, iondb.KeyTypeNumericSigned, 4, 4, 10)
    
    dict.Insert(3, 4)
    
    println(dict.Get(3)) // 4
   
    dict.Insert(4, 10)
    dict.Insert(5, 11)
    dict.Insert(5, 10)

    cursor := dict.Range(3, 6)
    for cursor.Next(); cursor.HasNext(); cursor.Next() {
        println("key: ", cursor.GetKey())
        println("val: ", cursor.GetValue())
    }
}
```
