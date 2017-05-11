package productstatus

import "fmt"

// ServiceBackend represents a Productstatus ServiceBackend resource.
type ServiceBackend struct {
	resource
}

// NewServiceBackend returns a ServiceBackend resource.
func NewServiceBackend() Resource {
	return &ServiceBackend{}
}

// GetServiceBackend returns a ServiceBackend resource.
func (c *Client) GetServiceBackend(uri string) (*ServiceBackend, error) {
	r, err := c.GetResource(uri)
	if err != nil {
		return nil, err
	}
	switch t := r.(type) {
	case *ServiceBackend:
		return t, nil
	default:
		return nil, fmt.Errorf("Resource is of wrong type %T", uri, t)
	}
}
