# Productshredder

Productshredder is a expired file removal utility.

It subscribes to `DataInstance` expiry messages from
[Productstatus](https://github.com/metno/productstatus), and deletes files from
physical media.

## Usage

A working environment with _Productstatus_ and _Kafka_ is assumed.
Productstatus will publish an _expiry message_, containing a list of
_DataInstance_ resources that have exceeded their time to live. Each message
contains DataInstances belonging to a specific combination of _Product_ and
_ServiceBackend_.

Productshredder can be configured to only allow deletion of DataInstances
belonging to a specific Product and ServiceBackend. This is done using the
`-products` and `-servicebackends` flags.

## Example

```
productshredder \
    -brokers localhost:9092 \
    -topic productstatus \
    -productstatus http://localhost:8000 \
    -username admin \
    -apikey 5bcf851f09bc65043d987910e1448781fcf4ea12 \
    -servicebackends /api/v1/servicebackend/495bb3be-e327-4840-accf-afefcd411e06/ \
    -products /api/v1/product/7d3fe736-5902-44d5-a34c-86f877190523/ \
    -verbose \
    -dry-run
```

The example above will run Productshredder against a local Kafka instance, on
the `productstatus` topic partition zero, and connect to a local Productstatus
instance. It filters input so that only DataInstances from the specified
Product and ServiceBackend are handled.
