A single append blob can at most consist of 50,000 appends of 4 MiB each.

~~~
9223372036854775808
9223372036854775807
       209715200000 bytes
             200000 MiB
                195 GiB

           43980465 * 50000 = 2199023255551
                              1099511627776 10^12
                              1000000000000 (trillion, a 1000 billions)

20000000 * 50000 = 1000000000000 (1 trillion, 10^12)

        1000000000000/2000 = 500000000
500000000/(3600*24*365.24) = 15.844477705172043141597407285722
~~~

If you sustain the max theoritcal write through, of about 2000 req/sec you (table storage limit), it will still take you more than 15 years to fill the event log. If you need a ginormous event log you should to look elsewhere for a solution.

Read performance is much better. More on this later.

An event transaction, cannot exceed more than 100 events or 4 MiB.

# scratch blob

To write an event into the storage account we first write the event data into the most recent scratch blob, this is done to establish a global total order of events. i.e. if event `A` was written into the scratch blob before event `B`, then event `A` happened before `B`.

That said, this does not mean that event `A` is guaranteed to commit. Whichever event transaction that commits first is what actually succeeds.

Note that, writing into the scratch blob is not the same as committing. Two or more non-conflicting transactions can commit out of order. While transaction `X` happened before transaction `Y`, transaction `Y` can commit before transaction `X`. When reading the event log from the scratch blob, it's important to not read non committed transactions.

# compact blobs

There are two kinds of compact blobs.

- `EventLogCompactBlob`
- `EventStreamCompactBlob`

The `EventLogCompactBlob` and `EventStreamCompactBlob` are constructed in a similar manner but the event log is constructed from the scratch blobs while the event stream is constructed using the event stream index.

To read event log, you read scratch blobs and check if the transaction was committed by loading the transaction commit entries. These should have been ordered so that you can query for them efficiently in batches. Reading the event log from the scratch blobs will result in several compacted blobs that represents the committed event log.

Event streams are constructed in a similar manner but the index serialized in a manner which that the event log is not.