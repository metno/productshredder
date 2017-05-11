package productstatus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"

	resty "gopkg.in/resty.v0"
)

// Client represents a Productstatus API client.
type Client struct {
	url *url.URL
}

// resource contains fields common to all Productstatus resources.
type resource struct {
	Id           string
	Slug         string
	Name         string
	Resource_uri string
	Created      string
	Modified     string
}

type Resource interface{}

// Product represents a Productstatus product resource.
type Product struct {
	resource
}

// New returns a new Productstatus client
func New(rawurl string) (*Client, error) {
	url, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	if url.Scheme != "http" && url.Scheme != "https" {
		return nil, fmt.Errorf("URL scheme must be HTTP or HTTPS")
	}
	if len(url.Host) == 0 {
		return nil, fmt.Errorf("Missing host from URL")
	}
	return &Client{
		url: url,
	}, nil
}

// readMessage parses a JSON marshalled Productstatus message, and returns a Message struct.
func unmarshalResource(data []byte) (Resource, error) {
	r := &Product{}

	reader := bytes.NewReader(data)
	decoder := json.NewDecoder(reader)

	if err := decoder.Decode(r); err != nil {
		return nil, err
	}

	return r, nil
}
func (c *Client) GetResource(uri string) (Resource, error) {
	data, err := c.Get(uri)
	if err != nil {
		return nil, err
	}
	return unmarshalResource(data)
}

// Get takes a URI, queries the server, checks the response code, and returns a
// byte slice with the object body.
func (c *Client) Get(uri string) ([]byte, error) {
	c.url.Path = uri
	url := c.url.String()
	resp, err := resty.R().Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("Got unexpected response code %s", resp.Status())
	}
	return resp.Body(), nil
}

// BasePath returns the start URI of all requests, currently at API version 1.
func (c *Client) BasePath() string {
	return `/api/v1/`
}

// ResourcePath returns the URI of a resource endpoint.
func (c *Client) ResourcePath(resource string) string {
	return c.BasePath() + resource + `/`
}
