package server

import (
	"bytes"
	"errors"
	"log"
)

const KB = 1024

const defaultChunkSize = 512 * KB // 64 KB

// this is the backend conflux structure [class]
type confluxInMemApi struct {
	// buf is the main in-mem storage for storing data
	// buf is intially a buffer of size = 0
	buf bytes.Buffer

	// restBuf is the rest of data that is read in the previous Consume operation but is not in a complete format to be returned so we waited for the next Consume
	// if we read 101\n102\n103 and we read only 6 bytes per chunk => so in the current consume we read "101\n10" but we will return "101\n" to be able to process the data correctly, and restBuf is "10" in the next consume we will read "2\n103" and we will need to append the restBuf before "2\n103" to have "102\n103" to be able to process it
	restBuf bytes.Buffer
}

func NewConfluxInMemApi() *confluxInMemApi {
	return &confluxInMemApi{}
}

func (cmem *confluxInMemApi) Produce(data []byte) error {
	_, err := cmem.buf.Write(data)
	if err != nil {
		log.Printf("error trying to send data to conflux buffer : %v\n", err)
		return err
	}

	// log.Printf("number of written bytes into the conflux buffer is = %v\n", numberOfWrittenBytes)
	return nil
}

func (cmem *confluxInMemApi) Consume(storage []byte) (result []byte, err error) {
	// if whoever the client using this api doesn't send any storage to save his data into, i will define a storage with the defaultChunkSize = 512 MB
	if storage == nil {
		storage = make([]byte, defaultChunkSize)
	}

	// to handle if we have any data from the previous Consume in the restBuf
	offset := int(0)

	// check the restBuf from the previous consume operation
	if cmem.restBuf.Len() > 0 {
		// check that the restBuf can fit within the data passed by the client so we avoid nil reference errors
		if cmem.restBuf.Len() > len(storage) {
			return nil, errors.New("length of buffer is larger than length of given slice to read the buffer on")
		}

		// read the rest of the data stored in the restBuf into the storage data
		// and then i will read the newely data offseted by the offset which is = numberOfButesFromPrevConsume
		numberOfBytesFromPrevConsume, err := cmem.restBuf.Read(storage)
		if err != nil {
			return nil, err
		}

		// reset the rest buf so we can add data into it if we faced the same problem in the newely read data
		cmem.restBuf.Reset()

		// set the offset to read the next portion into the data starting from it
		offset += numberOfBytesFromPrevConsume
	}

	// read the newely portion after reading the prev reminder portion
	// bytes.Buffer.Read(buf) => reads from the bytes.Buffer() a data with length = the length of the buf passed variable
	numberOfReadBytes, err := cmem.buf.Read(storage[offset:])
	if err != nil {
		return nil, err
	}

	// log.Printf("number of Read bytes from the conflux buffer is = %v\n", numberOfReadBytes)
	// log.Printf("Read bytes from the passed buffer is = %v\n", string(buf[0:numberOfReadBytes]))
	chunkOfData := storage[0 : numberOfReadBytes+offset]

	// we are consuming by reading from the original buffer storage with the length of the given chunk size (passed buffer)
	// but what if we have this case :
	// buffer = "100\n200\n" and the chunk-size is 6 bytes
	// so we will read in the first line this : "100\n2" and the next line will be "00\n" which will be a bad result so we need to return each time the data within the chunk size but also with \n at the end so we need to search for the last \n before returning the result
	// // if numberOfReadBytes == 0 {
	// // 	// its okay return the result
	// // 	return chunkOfData, nil // if we read zero bytes in the newely data, we shouldn't pass this to the ProcessConsumeData() method because we will face a nil dereference error because we will try to access chunkOfData[0-1] !!
	// // }

	toLastNewLine, restOfData, err := ProcessConsumedData(chunkOfData, numberOfReadBytes+offset)
	if err != nil {
		return nil, err
	}
	// // // reset the restBuf
	// // cmem.restBuf.Reset() // already reseted

	// and write to it the protion of data that is read partially ("10" and next consume will give us "2\n103")
	if restOfData != nil {
		// then reset it
		cmem.restBuf.Write(restOfData)
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
