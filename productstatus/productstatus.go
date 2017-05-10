package productstatus

import (
	"fmt"
	"net/url"
)

type Client struct {
	url *url.URL
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
