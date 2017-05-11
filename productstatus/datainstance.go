package productstatus

import "fmt"

// DataInstance represents a DataInstancestatus DataInstance resource.
type DataInstance struct {
	resource
	Url     string
	Expires string
}

// NewDataInstance returns a DataInstance resource.
func NewDataInstance() Resource {
	return &DataInstance{}
}

// GetDataInstance returns a DataInstance resource.
func (c *Client) GetDataInstance(uri string) (*DataInstance, error) {
	r, err := c.GetResource(uri)
	if err != nil {
		return nil, err
	}
	switch t := r.(type) {
	case *DataInstance:
		return t, nil
	default:
		return nil, fmt.Errorf("Resource is of wrong type %T", uri, t)
	}
}
