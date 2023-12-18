package main

import (
	"bytes"
	"encoding/binary"
)

type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	*dal

	pageNum    pgnum
	items      []*Item
	childNodes []pgnum
}

func NewEmptyNode() *Node {
	return &Node{}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1
	// Add page header: isLeaf, key-value pairs count, node num
	// isLeaf
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	//key value pair count
	numberOfKeyValuePairs := len(n.items)
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(numberOfKeyValuePairs))
	leftPos += 2

	for i := 0; i < len(n.items); i++ {
		if !isLeaf {
			childNode := n.childNodes[i]
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}
		item := n.items[i]
		klen := len(item.key)
		vlen := len(item.value)

		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)
		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)
		rightPos -= 1
		buf[rightPos] = byte(klen)
	}
	if !isLeaf {
		// Write the last child node
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		// Write the child page as a fixed size of 8 bytes
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftPos := 0
	isLeaf := uint16(buf[0])
	numberOfKeyValue := binary.LittleEndian.Uint16(buf[1:3])
	n.isLeaf()
	leftPos += 3

	// Read body
	for i := 0; i < int(numberOfKeyValue); i++ {
		if isLeaf == 0 { //not a leaf
			n.childNodes = append(n.childNodes, pgnum(binary.LittleEndian.Uint64(buf[leftPos:])))
			leftPos += pageNumSize
		}

		//read offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[offset])
		offset += 1
		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[offset])
		offset += 1
		value := buf[offset : offset+vlen]
		offset += vlen

		n.items = append(n.items, newItem(key, value))
	}

	if isLeaf == 0 { //not a leaf
		// Read the last child node
		n.childNodes = append(n.childNodes, pgnum(binary.LittleEndian.Uint64(buf[leftPos:])))
	}
}

func (n *Node) writeNode(node *Node) *Node {
	r, _ := n.dal.writeNode(node)
	return r
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.dal.getNode(pageNum)
}

// search
func (n *Node) findInNode(key []byte) (bool, int) {
	for i, item := range n.items {
		res := bytes.Compare(item.key, key)
		if res == 0 {
			return true, i
		}

		//first instance of item.key > key
		if res == 1 {
			return false, i
		}
	}
	// every shit key was < key so we return the total length
	return false, len(n.items)
}

func (n *Node) findKey(key []byte) (int, *Node, error) {
	i, node, err := n.findKeyHelper(n, key)
	if err != nil {
		return -1, nil, err
	}
	return i, node, nil
}

func (n *Node) findKeyHelper(node *Node, key []byte) (int, *Node, error) {
	found, i := node.findInNode(key)
	//base condition 1
	if found {
		return i, node, nil
	}
	//base condition 2
	if node.isLeaf() {
		return -1, nil, nil
	}
	//recurrence
	child, err := node.getNode(n.childNodes[i])
	if err != nil {
		return -1, nil, err
	}
	return n.findKeyHelper(child, key)
}

// elementSize returns the size of a key-value-childNode triplet at a given index.
// If the node is a leaf, then the size of a key-value pair is returned.
// It's assumed i <= len(n.items)
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumSize // 8 is the pgnum size
	return size
}

// nodeSize returns the node's size in bytes
func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.items {
		size += n.elementSize(i)
	}

	// Add last page
	size += pageNumSize // 8 is the pgnum size
	return size
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex {
		n.items = append(n.items, item)
		return insertionIndex
	}
	n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
	n.items[insertionIndex] = item
	return insertionIndex
}

// isOverPopulated checks if the node size is bigger than the size of a page.
func (n *Node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

// isUnderPopulated checks if the node size is smaller than the size of a page.
func (n *Node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}
