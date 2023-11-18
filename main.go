package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/FadyGamilM/conflux/client"
	"github.com/FadyGamilM/conflux/ports"
	"github.com/FadyGamilM/conflux/server"
	"github.com/FadyGamilM/conflux/web"
	"github.com/gin-gonic/gin"
)

func main() {
	// confluxClient := client.NewConfluxClient([]string{"localhost:8080"})
	confluxServer := server.NewConfluxInMemApi()
	// =======> INTEGRATION TEST #1
	// expected, err := send(confluxServer)
	// if err != nil {
	// 	log.Fatalf("error sending data to conflux : %v\n", err)
	// }
	// log.Printf("expected ➜ %v\n", expected)

	// actual, err := receive(confluxServer)
	// if err != nil {
	// 	log.Fatalf("error receiving data from conflux : %v\n", err)
	// }
	// log.Printf("actual ➜ %v\n", actual)

	webApi := web.NewServer(gin.Default(), confluxServer)
	webApi.SetupEndpoints()
	ctx := context.Background()
	webApi.Run(ctx, "localhost", "9090")
	<-ctx.Done()
}

var numberOfValuesToProduce = int64(10000000) // number of values we will send
var maxChunkSize = 64 * client.KB             // max chunk size = 1 MB = 1024 KB  ==> i will make it 64 KB for now

func receive(s ports.ConfluxInMemApi) (sum int64, err error) {
	// define a 1MB bufer to receive data on by sending it into the Consume method
	buf := make([]byte, maxChunkSize)
	// infinite loop
	for {
		// read data from conflux buffer with size of this buf [1 MB data per batch]
		batcheOfData, err := s.Consume(buf)
		if err == io.EOF {
			return sum, nil
		} else if err != nil {
			return 0, err
		}
		// split data into lines
		ints := strings.Split(string(batcheOfData), "\n")
		// loop through the lines
		for _, str := range ints {
			// avoid processing empty lines
			if str == "" {
				continue
			}
			// convert into number from string
			i, err := strconv.Atoi(str)
			if err != nil {
				return 0, err
			}
			sum += int64(i)
		}
	}
}

/*
 1. define buffer b to store data on
 2. loop from 0 -> maxN where maxN is the number of times we will send a value, and this value is added to
    the buffer if the buffer doesn't exceed the chunk size and if its exceeded we send the chuck of data inside the buffer to the Conflux and then restart the buffer to continuo
    2.1. Each iternation we do the following :
    2.1.1. add the current data item into a sum variable to get the sum of all numbers we will send
    (Assuming data are only ints for now)
    2.1.2. add the current data item into the buffer with a "\n" after it "for easy splitting later"
    2.1.3. check if we reached the max chuck size ?
    2.1.4. if we reached it we send the data to the buffer by passing the buffer.bytes to the send()
    method which accepts a slice of bytes, then we reset the buffer to start with new one in the next iteration
    2.1.5. if we didn't reach it we go to the next iteration..
*/
func send(s ports.ConfluxInMemApi) (sum int64, err error) {
	var b bytes.Buffer
	for i := 0; i <= int(numberOfValuesToProduce); i++ {
		sum += int64(i)
		fmt.Fprintf(&b, "%d\n", i)
		if b.Len() >= maxChunkSize {
			// logs
			// log.Printf("we reached the max chunk size, producing to conflux ➜ %v\n", b.Len())
			if err := s.Produce(b.Bytes()); err != nil {
				return 0, err
			}
			b.Reset()
		}
	}
	// flush the rest of the buffer
	if b.Len() != 0 {
		if err := s.Produce(b.Bytes()); err != nil {
			return 0, err
		}
	}
	return sum, nil
}
