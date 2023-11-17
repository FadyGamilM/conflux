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
	// restBuf is the rest of data that is read in the previous Consume operation but is not in a complete format to be returned so we waited fro the next Consume
	// if we read 101\n102\n103 and we read only 6 bytes => so in the current consume we read "101\n10" but we will return "101\n" and restBuf is "10" in the next consume we will read "2\n103" and we will need to append the restBuf before "2\n103" to have 102\n103
	restBuf bytes.Buffer
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
func (c *ConfluxClientApi) Consume(data []byte) ([]byte, error) {
	if data == nil {
		data = make([]byte, defaultChunkSize)
	}

	offset := int(0)

	// check the restBuf from the previous consume operation
	if c.restBuf.Len() > 0 {
		// check that the restBuf can fit within the data passed by the client so we avoid nil reference errors
		if c.restBuf.Len() > len(data) {
			return nil, errors.New("length of buffer is larger than length of given slice to read the buffer on")
		}

		// read it into a temp slice of bytes to append it later to the newely consumed portion before start processing the newely created portion
		numberOfBytesFromPrevConsume, err := c.restBuf.Read(data)
		if err != nil {
			return nil, err
		}
		// reset the rest buf
		c.restBuf.Reset()

		// set the offset to read the next portion into the data starting from it
		offset += numberOfBytesFromPrevConsume
	}

	// read the newely portion after reading the prev reminder portion
	// bytes.Buffer.Read(buf) => reads from the bytes.Buffer() a data with length = the length of the buf passed variable
	numberOfReadBytes, err := c.buf.Read(data[offset:])
	if err != nil {
		return nil, err
	}

	// log.Printf("number of Read bytes from the conflux buffer is = %v\n", numberOfReadBytes)
	// log.Printf("Read bytes from the passed buffer is = %v\n", string(buf[0:numberOfReadBytes]))
	chunkOfData := data[0 : numberOfReadBytes+offset]

	// we are consuming by reading from the original buffer storage with the length of the given chunk size (passed buffer)
	// but what if we have this case :
	// buffer = "100\n200\n" and the chunk-size is 6 bytes
	// so we will read in the first line this : "100\n2" and the next line will be "00\n" which will be a bad result so we need to return each time the data within the chunk size but also with \n at the end so we need to search for the last \n before returning the result
	if numberOfReadBytes == 0 {
		// its okay return the result
		return data[0:numberOfReadBytes], nil
	}

	toLastNewLine, restOfData, err := ProcessConsumedData(chunkOfData, numberOfReadBytes+offset)
	if err != nil {
		return nil, err
	}
	// reset the restBuf
	c.restBuf.Reset()

	// and write to it the protion of data that is read partially ("10" and next consume will give us "2\n103")
	if restOfData != nil {
		// then reset it
		c.restBuf.Write(restOfData)
	}
	return toLastNewLine, nil
}

// this method takes a []byte and process it by returning the data from the first byte to the last \n byte, and returns the rest and returns an error
func ProcessConsumedData(chunkOfData []byte, numberOfReadBytes int) (toLastBreakLine, rest []byte, err error) {
	// check if we read it right and the last byte is \n and we get luckey
	if chunkOfData[numberOfReadBytes-1] == '\n' {
		return chunkOfData, nil, nil
	}

	// now check and find the last \n
	lastNewLine := bytes.LastIndexByte(chunkOfData, '\n')
	if lastNewLine == -1 {
		return nil, nil, errors.New("data is corrupted, couldn't find a \n to parse the data correctly")
	}

	// return the right portion for this chunk
	return chunkOfData[0 : lastNewLine+1], chunkOfData[lastNewLine+1:], nil
}
