package productstatus

import "fmt"

// Product represents a Productstatus Product resource.
type Product struct {
	resource
}

// NewProduct returns a Product resource.
func NewProduct() Resource {
	return &Product{}
}

// GetProduct returns a Product resource.
func (c *Client) GetProduct(uri string) (*Product, error) {
	r, err := c.GetResource(uri)
	if err != nil {
		return nil, err
	}
	switch t := r.(type) {
	case *Product:
		return t, nil
	default:
		return nil, fmt.Errorf("Resource is of wrong type %T", uri, t)
	}
}
