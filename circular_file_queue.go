package fqueue

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

type circularFileQueue struct {
	file  *os.File
	m     mmap.MMap
	start uint32
	end   uint32
	count uint32
	lock  sync.RWMutex
	cond  *sync.Cond
}

const (
	startTagPos uint32 = 0
	endTagPos   uint32 = 1 << 2
	countTagPos uint32 = 1 << 4

	headPos = countTagPos

	maxFileSize uint32 = 5 * 1024 * 1024
	preLength   uint32 = 4
	tagLength   uint32 = 4
)

var _ Queue = (*circularFileQueue)(nil)

func NewCircularFileQueue(name string) (Queue, error) {
	res := &circularFileQueue{}
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(int64(maxFileSize)); err != nil {
		file.Close()
		return nil, err
	}
	res.file = file

	m, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	res.m = m

	res.start = binary.BigEndian.Uint32(m[startTagPos : startTagPos+tagLength])
	res.end = binary.BigEndian.Uint32(m[endTagPos : endTagPos+tagLength])
	res.count = binary.BigEndian.Uint32(m[countTagPos : countTagPos+tagLength])
	if res.start == 0 {
		res.start = countTagPos + tagLength
	}
	if res.end == 0 {
		res.end = countTagPos + tagLength
	}

	if res.start > res.end {
		return nil, ErrInvalidQueue
	}

	res.cond = sync.NewCond(&res.lock)

	return res, nil
}

func (q *circularFileQueue) IsEmpty() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return q.count == 0
}

func (q *circularFileQueue) Size() int {
	q.lock.RLock()
	defer q.lock.RUnlock()

	return int(q.count)
}

func (q *circularFileQueue) Pop() []byte {
	q.lock.Lock()
	for q.count == 0 {
		q.cond.Wait()
	}

	var length uint32
	if maxFileSize-q.start >= preLength {
		length = binary.BigEndian.Uint32(q.m[q.start : q.start+preLength])
	} else {
		buf := make([]byte, preLength)
		copy(buf[:maxFileSize-q.start], q.m[q.start:])
		copy(buf[maxFileSize-q.start:], q.m[headPos:headPos+(maxFileSize-q.start)])
		length = binary.BigEndian.Uint32(buf)
	}

	res := make([]byte, length)
	if maxFileSize-q.start-preLength >= length {
		copy(res, q.m[q.start+preLength:q.start+preLength+length])
		q.start = q.start + preLength + length
	} else {
		copy(res[:maxFileSize-q.start-preLength], q.m[q.start+preLength:maxFileSize])
		remain := length - (maxFileSize - q.start - preLength)
		copy(res[maxFileSize-q.start-preLength:], q.m[headPos:headPos+remain])
		q.start = headPos + remain
	}
	binary.BigEndian.PutUint32(q.m[startTagPos:startTagPos+tagLength], q.start)

	q.count--
	binary.BigEndian.PutUint32(q.m[countTagPos:countTagPos+tagLength], q.count)

	q.lock.Unlock()

	return res
}

func (q *circularFileQueue) Push(data []byte) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	needLen := preLength + uint32(len(data))
	if needLen > maxFileSize-q.end+q.start-headPos {
		return ErrNotEnoughSpace
	}

	if maxFileSize-q.end >= needLen {
		q.end = q.pushBackEnough(data)
	} else {
		q.end = q.pushBackNotEnough(data)
	}
	binary.BigEndian.PutUint32(q.m[endTagPos:endTagPos+tagLength], q.end)

	q.count++
	binary.BigEndian.PutUint32(q.m[countTagPos:countTagPos+tagLength], q.count)

	q.cond.Signal()

	return nil
}

func (q *circularFileQueue) pushBackEnough(data []byte) uint32 {
	length := uint32(len(data))
	binary.BigEndian.PutUint32(q.m[q.end:q.end+preLength], length)
	copy(q.m[q.end+preLength:q.end+preLength+length], data)

	return q.end + preLength + length
}

func (q *circularFileQueue) pushBackNotEnough(data []byte) uint32 {
	length := uint32(len(data))
	if maxFileSize-q.end >= preLength {
		binary.BigEndian.PutUint32(q.m[q.end:q.end+preLength], length)
		copy(q.m[q.end+preLength:maxFileSize], data[:maxFileSize-q.end-preLength])
		remain := length - (maxFileSize - q.end - preLength)
		copy(q.m[headPos:headPos+remain], data[maxFileSize-q.end-preLength:])

		return headPos + remain
	}

	buf := make([]byte, preLength)
	binary.BigEndian.PutUint32(buf, length)
	copy(q.m[q.end:maxFileSize], buf)
	remain := preLength - (maxFileSize - q.end)
	copy(q.m[headPos:headPos+remain], buf[maxFileSize-q.end:])
	copy(q.m[headPos+remain:headPos+remain+length], data)

	return headPos + length + remain
}

func (q *circularFileQueue) Close() error {
	if err := q.m.Unmap(); err != nil {
		q.file.Close()
		return err
	}

	return q.file.Close()
}
