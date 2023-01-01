package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/araji/proglog/internal/server"
)

var httpServer *http.Server

var _ = BeforeSuite(func() {
	httpServer = server.NewHTTPServer(":8080")
	go httpServer.ListenAndServe()
})
var _ = AfterSuite(func() {
	httpServer.Close()
})

var _ = Describe("Http", func() {

	//produces logs ad returns offset
	Describe("producing/consuming logs", func() {
		It("Acceptance test: round trip", func() {
			data, err := json.Marshal(server.ProduceRequest{
				Record: server.Record{
					Value: []byte("hello world"),
				},
			})
			produceResponse := &server.ProduceResponse{}
			Expect(err).NotTo(HaveOccurred())
			req, _ := http.NewRequest("POST", "http://localhost:8080/", bytes.NewReader(data))
			res, err := http.DefaultClient.Do(req)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.StatusCode).To(Equal(http.StatusOK))
			json.NewDecoder(res.Body).Decode(produceResponse)
			Expect(produceResponse.Offset).To(Equal(uint64(0)))

			data2, _ := json.Marshal(server.ConsumeRequest{
				Offset: 0,
			})

			consumeResponse := &server.ConsumeResponse{}
			req2, _ := http.NewRequest("GET", "http://localhost:8080/", bytes.NewReader(data2))
			res2, err2 := http.DefaultClient.Do(req2)
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2.StatusCode).To(Equal(http.StatusOK))
			json.NewDecoder(res2.Body).Decode(consumeResponse)
			Expect(consumeResponse.Record.Value).To(Equal([]byte("hello world")))

		})
	})

})
