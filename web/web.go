package web

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/FadyGamilM/conflux/ports"
	"github.com/gin-gonic/gin"
)

type ConfluxWebServer struct {
	r          *gin.Engine
	confluxApi ports.ConfluxInMemApi
}

func NewServer(r *gin.Engine, ca ports.ConfluxInMemApi) *ConfluxWebServer {
	return &ConfluxWebServer{
		r:          r,
		confluxApi: ca,
	}
}

func (cws *ConfluxWebServer) SetupEndpoints() {
	cws.r.POST("/api/v1/produce", HandleSendingData(cws.confluxApi))
	cws.r.GET("/api/v1/consume", HandleReceivingData(cws.confluxApi))
}

func (cws *ConfluxWebServer) Run(ctx context.Context, host, port string) context.Context {
	srv := *&http.Server{
		Addr:    fmt.Sprintf("%v:%v", host, port),
		Handler: cws.r,
	}

	ctx, cancel := context.WithCancel(ctx)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Println("couldn't start the server")
			cancel() // => i will block at <- ctx.Done() so when i here anything from this go routine, i will shutodwn the server
		}
	}()

	return ctx
}
