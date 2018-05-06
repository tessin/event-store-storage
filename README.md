`CloudEventStore` is an event store built on top of Azure Storage service, purposefully built to be used from within an Azure Function. Not setting any records but it is cheap and reliable.

---
Please note that this is work in progress and while it is functional consider this to be some kind of late alpha, early beta and subject to breaking changes.

---

# Limits

- 1.6 trillion events <sup>1</sup>
- 4 MiB, or 99 events per transaction

> <sup>1</sup> Note that you have to sustain the theoretical max 2000 req/s for 15 years to archive this and at that rate your Azure Storage transaction bill alone is $800 USD per day!

# Cost

The actual cost of the `CloudEventStore` is roughly that of a blob and table transaction. Those are billed at pennies ($0.054 + $0.00036) respectively per 10,000 transactions <sup>2</sup>.

- `Append` 1 write blob operation, 1 table operation
- `GetLog` 1 read blob operation, 1 table operation
- `GetStream` N read blob operations, 1 table operation <sup>3</sup>

If you want something other than `LRS` you pay more for the data at rest.

> <sup>2</sup> [Azure Storage pricing](https://duckduckgo.com/?q=Azure+Storage+pricing) retrieved, 2018-05-06 

> <sup>3</sup> `N` is here a bit misleading since getting an event stream requires fetching data that wasn't necessarily committed close in space. `N` is just an approximation for the number of requests that need to be issued for non-continuous ranges of bytes. It also varies with the batch size. Note that read blob operations are roughly 1 order of magnitude cheaper than blob write operations.

# Ugly

All table operations use the same partition key. This was necessary to be able to support transactions and limits the theoretical max append throughput to 2000 requests/s.

The read side of things is -- _shall we say_ -- infinitely scalable because having an immutable record of totally ordered events allows us to derive any additional data representation that we might need. Even if reading an event stream is an horrible I/O pattern we can have any number of worker processes that run single writer jobs that build secondary indexes for event logs streams. There is an additional cost associated with this but it allows read throughput up to 60 MiB per second, or up to 500 requests per second (per log or stream independently) up to the limits of the storage account itself, which up on request can be increased further by Azure Support.

In summary, the read side of things is more complex but also very scalable.

# API

- `Append` append a set of events as a single transaction
- `GetLog` get a segment of events from the log
- `GetStream` get a segment of events from a specific stream

## Event

- `id` global ID
- `sid` stream UUID
- `n` local ID
- `tid` type UUID
- `t` timestamp
- `b` payload (binary/base64)

The global ID provides a total ordering while the local ID is used for optimistic concurrency control.

The payload can be any binary data.