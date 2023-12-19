
# slimDB

A storage engine written in Golang for experimental and educational purpose. Persistent BTree is programmed for minimum disk seek.




## Run Locally

Clone the project

```bash
  git clone https://github.com/kevinjad/slim-storage-engine.git
```

Go to the project directory

```bash
  cd slim-storage-engine
```

Build

```bash
  go build
```

run example

```bash
  ./slim-storage-engine
```


## Usage/Examples

```go

settings := &Settings{
    pageSize:       os.Getpagesize(),
    MinFillPercent: 0.0125,
    MaxFillPercent: 0.025,
}
dal, _ := newDal("./slim.db", settings)

c := newCollection([]byte("collection1"), dal.root)
c.dal = dal

_ = c.Put([]byte("Key1"), []byte("Value1"))
item, _ := c.Find([]byte("Key1"))

fmt.Printf("key is: %s, value is: %s\n", item.key, item.value)
dal.writeFreelist()
_ = dal.close()


```

