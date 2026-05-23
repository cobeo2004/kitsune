package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	baseURL := os.Getenv("KITSUNE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	must(request(http.MethodPost, baseURL+"/v1/indexes", `{"name":"books","shardCount":3,"replicationFactor":2,"mappingVersion":1,"mapping":{"defaultAnalyzer":"standard"}}`))
	must(request(http.MethodPut, baseURL+"/v1/indexes/books/documents/doc-1", `{"title":"Bleve distributed search"}`))
	must(waitForSearchHit(baseURL, "doc-1", 30*time.Second))
	must(expectClusterStatus(baseURL))
}

func request(method, url, body string) error {
	_, err := requestBytes(method, url, body)
	return err
}

func requestBytes(method, url, body string) ([]byte, error) {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	if body != "" {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s %s read response body: %w", method, url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s returned %s: %s", method, url, resp.Status, string(data))
	}
	return data, nil
}

type searchResponse struct {
	Hits []struct {
		DocumentID string `json:"documentId"`
	} `json:"hits"`
}

func waitForSearchHit(baseURL, documentID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		data, err := requestBytes(http.MethodGet, baseURL+"/v1/indexes/books/search?q=Bleve&limit=10", "")
		if err == nil {
			var resp searchResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				return fmt.Errorf("decode search response: %w", err)
			}
			for _, hit := range resp.Hits {
				if hit.DocumentID == documentID {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("search did not return %q before timeout: %w", documentID, err)
			}
			return fmt.Errorf("search did not return %q before timeout", documentID)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type clusterStatus struct {
	State       string `json:"state"`
	Nodes       []any  `json:"nodes"`
	Tablets     []any  `json:"tablets"`
	Assignments []any  `json:"assignments"`
}

func expectClusterStatus(baseURL string) error {
	data, err := requestBytes(http.MethodGet, baseURL+"/v1/cluster/status", "")
	if err != nil {
		return err
	}
	var status clusterStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return fmt.Errorf("decode cluster status: %w", err)
	}
	if status.State != "ready" {
		return fmt.Errorf("cluster state = %q, want ready", status.State)
	}
	if len(status.Nodes) < 3 {
		return fmt.Errorf("cluster nodes = %d, want at least 3", len(status.Nodes))
	}
	if len(status.Tablets) == 0 || len(status.Assignments) == 0 {
		return fmt.Errorf("cluster status missing tablets or assignments")
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
