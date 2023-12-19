package main

import (
	"fmt"
	"os"
)

func main() {
	settings := &Settings{
		pageSize:       os.Getpagesize(),
		MinFillPercent: 0.0125,
		MaxFillPercent: 0.025,
	}
	dal, _ := newDal("./mainTest", settings)

	c := newCollection([]byte("collection1"), dal.root)
	c.dal = dal

	_ = c.Put([]byte("Key1"), []byte("Value1"))
	_ = c.Put([]byte("Key2"), []byte("Value2"))
	_ = c.Put([]byte("Key3"), []byte("Value3"))
	_ = c.Put([]byte("Key4"), []byte("Value4"))
	_ = c.Put([]byte("Key5"), []byte("Value5"))
	_ = c.Put([]byte("Key6"), []byte("Value6"))
	keys := []string{"Key1", "Key2", "Key3", "Key4", "Key5", "Key6"}
	for _, key := range keys {
		item, _ := c.Find([]byte(key))

		fmt.Printf("key is: %s, value is: %s\n", item.key, item.value)
	}

	item, _ := c.Find([]byte("Key1"))

	fmt.Printf("key is: %s, value is: %s\n", item.key, item.value)
	dal.writeFreelist()
	_ = dal.close()
}
