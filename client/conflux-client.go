package client

import (
	"bytes"
	"errors"
	"log"
)

const KB = 1024

const defaultChunkSize = 64 * KB // 64 KB

type ConfluxClientApi struct {
	addrs []string
	// buf is intially a buffer of size = 0
	buf bytes.Buffer
}

// factory method to create new conflux client api
func NewConfluxClient(addresses []string) *ConfluxClientApi {
	return &ConfluxClientApi{
		addrs: addresses,
	}
}

// add new data to the buffer storage of the conflux
/*
 - receives : a slice of bytes [with chunk-size]
 - returns : error
*/
func (c *ConfluxClientApi) Produce(d []byte) error {
	_, err := c.buf.Write(d)
	if err != nil {
		log.Printf("error trying to send data to conflux buffer : %v\n", err)
		return err
	}

	// log.Printf("number of written bytes into the conflux buffer is = %v\n", numberOfWrittenBytes)
	return nil
}

// consumes batches from the buffer storage
func (c *ConfluxClientApi) Consume(buf []byte) ([]byte, error) {
	if buf == nil {
		buf = make([]byte, defaultChunkSize)
	}

	// bytes.Buffer.Read(buf) => reads from the bytes.Buffer() a data with length = the length of the buf passed variable
	numberOfReadBytes, err := c.buf.Read(buf)
	if err != nil {
		return nil, err
	}

	// log.Printf("number of Read bytes from the conflux buffer is = %v\n", numberOfReadBytes)
	// log.Printf("Read bytes from the passed buffer is = %v\n", string(buf[0:numberOfReadBytes]))
	chunkOfData := buf[0:numberOfReadBytes]

	// we are consuming by reading from the original buffer storage with the length of the given chunk size (passed buffer)
	// but what if we have this case :
	// buffer = "100\n200\n" and the chunk-size is 6 bytes
	// so we will read in the first line this : "100\n2" and the next line will be "00\n" which will be a bad result so we need to return each time the data within the chunk size but also with \n at the end so we need to search for the last \n before returning the result
	if numberOfReadBytes == 0 {
		// its okay return the result
		return buf[0:numberOfReadBytes], nil
	}

	// check if we read it right and the last byte is \n and we get luckey
	if chunkOfData[numberOfReadBytes-1] == '\n' {
		return chunkOfData, nil
	}

	// now check and find the last \n
	lastNewLine := bytes.LastIndexByte(chunkOfData, '\n')
	if lastNewLine == -1 {
		return nil, errors.New("data is corrupted, couldn't find a \n to parse the data correctly")
	}

	// return the right portion for this chunk
	return chunkOfData[0 : lastNewLine+1], nil
}
