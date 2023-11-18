package ports

type ConfluxInMemApi interface {
	Produce(data []byte) error
	Consume(storage []byte) (result []byte, err error)
}
