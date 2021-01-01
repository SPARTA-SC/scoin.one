package rpc

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"testing"
)

func TestClient_GetConstants(t *testing.T) {
	client, err := NewClient(nil, "https://mainnet-tezos.giganode.io")
	assert.NoError(t, err)

	constants, err := client.GetConstantsHeight(context.Background(), 1)
	assert.NoError(t, err)
	t.Log(constants)
	// mon := NewBlockHeaderMonitor()
	// err = client.MonitorBlockHeader(context.Background(),mon)
	// if err != nil {
	// 	t.Log(err)
	// }
	// for {
	// 	head, err := mon.Recv(context.Background())
	// 	if err != nil {
	// 		t.Log(err)
	// 		return
	// 	}
	// 	t.Log(head.Level)
	// }
}

func TestClient_GetBlockHeight(t *testing.T) {
	proxyUrl, err := url.Parse("http://127.0.0.1:8001")
	tr := &http.Transport{Proxy: http.ProxyURL(proxyUrl)}
	httpClient := &http.Client{Transport: tr}
	client, err := NewClient(httpClient, "https://mainnet-tezos.giganode.io")
	assert.NoError(t, err)
	block, err := client.GetBlockHeight(context.Background(), 1)
	assert.NoError(t, err)
	t.Log(*block)
	for _, v := range block.Header.Content.Parameters.Accounts {
		t.Log(*v)
	}
	for _, v := range block.Header.Content.Parameters.Contracts {
		t.Log(*v)
	}
	for _, v := range block.Header.Content.Parameters.Commitments {
		t.Log(*v)
	}
	t.Log(block.Header.Content.Parameters.Supply())

}
