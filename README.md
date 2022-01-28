# shq - Shared inter-process queues

`shq` requires OTP 24 or later.

## Usage

### Starting a queue

```erlang
%% Start a non-linked queue
{ok, Pid} = shq:start(Opts).
{ok, Pid} = shq:start(ServerName, Opts).

%% Start a linked queue
{ok, Pid} = shq:start_link(Opts).
{ok, Pid} = shq:start_link(ServerName, Opts).

%% Start a monitored queue
{ok, {Pid, Mon}} = shq:start_monitor(Opts).
{ok, {Pid, Mon}} = shq:start_monitor(ServerName, Opts).
```

`ServerName` is a server name as used when starting a named `gen_server`.

`Opts` is a map or property list of options.
Currently, the only accepted option is `max`, which is the maximum number of items the queue can hold.
Allowed values are either a non-negative integer or the atom `infinity` (default: `infinity`).

### Inserting an item

```erlang
%% Insert an item at the rear
shq:in(ServerRef, Item).
shq:in(ServerRef, Item, Timeout).

%% Insert an item at the front
shq:in_r(ServerRef, Item).
shq:in_r(ServerRef, Item, Timeout).
```

`ServerRef` is a server reference as used with `gen_server`.

`Item` is the item to insert.

`Timeout` is a timeout to wait for a slot to become available if the queue is full (default: `0`).

The return value is either the atom `ok` if the item was queued, or the atom `full` if not.

When these functions return `full`, it is guaranteed that the given item was not inserted, and
will not be inserted later without calling this function again.

### Retrieving an item

```erlang
%% Retrieving an item from the front
shq:out(ServerRef).
shq:out(ServerRef, Timeout).

%% Retrieving an item from the rear
shq:out_r(ServerRef).
shq:out_r(ServerRef, Timeout).
```

For `ServerRef`, see "Inserting".

`Timeout` is a timeout to wait for an item to become available if the queue is empty (default: `0`).

The return value is either a tuple `{ok, Item}`, or the atom `empty`.

When these functions return `empty`, it is guaranteed that no item was removed from the queue, and
none will be removed later without calling this function again.

### Peeking

```erlang
%% Peek at the front
shq:peek(ServerRef).

%% Peek at the rear
shq:peek_r(ServerRef).
```

Peeking is the same as retrieving, but without removing the item from the queue.
The operation will always return instantly, ie there is no way to wait for an item to become available
if the queue is empty.

### Retrieving queue size

```erlang
shq:size(ServerRef).
```

Retrieves the number of items in the queue.

The return value is a non-negative integer.

### Stopping a queue

```erlang
ok=shq:stop(ServerRef).
```

All items left will be lost when a queue is stopped.


## Notes

As `shq` queues are backed by `ets` tables of type `set`, the number of items in a queue does not affect its performance.

Queue performance may be affected by how you use them, however.
