- What if the uncommitted blob is filled with only uncommitted transactions?

- When deployed into an Azure Function we can run background tasks that clean up uncommitted blocks and maintain indexes.

- Make the max number of blocks in an append blob configurable for testing purposes. Best effort. Append blob is available as long as block count <= limit but the eventual block count can be grater than the limit due to concurrency. When used like this the upper limit on the event store capacity is less as well.

- Fast path/slow path the first time you access the event log or an event stream the backing storage may not be ready. If the backing storage needs to be updated the request can take a little extra time doing so. We can use storage queues to preemptively do this. Configurable.