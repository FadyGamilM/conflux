package web

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/FadyGamilM/conflux/ports"
	"github.com/gin-gonic/gin"
)

type produceDataReq struct {
	Data string `json:"data"`
}

type consumeDataRes struct {
	Response string `json:"response"`
}

func HandleSendingData(confluxServer ports.ConfluxInMemApi) gin.HandlerFunc {
	return func(c *gin.Context) {

		reqData := produceDataReq{}
		if err := c.ShouldBindJSON(&reqData); err != nil {
			c.JSON(
				http.StatusBadRequest,
				gin.H{
					"error": fmt.Sprintf("error trying to bind req body : %v\n", err),
				},
			)
			return
		}

		reqData.Data = reqData.Data + "\n"
		if err := confluxServer.Produce([]byte(reqData.Data)); err != nil {
			c.JSON(
				http.StatusInternalServerError,
				gin.H{
					"error": fmt.Sprintf("business logic error, working on it : %v\n", err),
				},
			)
			return
		}

		c.JSON(
			http.StatusCreated,
			gin.H{
				"response": "data is sent to conflux server",
			},
		)
		return
	}
}

func HandleReceivingData(confluxServer ports.ConfluxInMemApi) gin.HandlerFunc {
	return func(c *gin.Context) {

		// TODO => refactor this to be a service layer logic
		// define a bufer to receive the data on it
		buf := make([]byte, 512*1024)

		batchOfData, err := confluxServer.Consume(buf)
		log.Println(batchOfData)
		if err == io.EOF {
			c.JSON(
				http.StatusOK,
				gin.H{
					"response": string(batchOfData),
				},
			)
			return
		} else if err != nil {
			c.JSON(
				http.StatusInternalServerError,
				gin.H{
					"error": fmt.Sprintf("business logic error, working on it : %v\n", err),
				},
			)
			return
		}
		c.JSON(
			http.StatusOK,
			gin.H{
				"response": string(batchOfData),
			},
		)
		return

	}
}
