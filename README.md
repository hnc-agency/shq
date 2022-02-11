# shq - Shared inter-process queues

 ![Tests](https://github.com/hnc-agency/shq/actions/workflows/ci.yml/badge.svg)
 :link: [Hex package](//hex.pm/packages/shq)

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

<dl>
  <dt><code>ServerName</code></dt>
  <dd>A server name as used when starting a named <code>gen_server</code>.</dd>
  <dt><code>Opts</code></dt>
  <dd>A map or property list of options
    <dl>
      <dt><code>max => non_neg_integer() | infinity</code></dt>
      <dd>The maximum number of items the queue can hold (default: <code>infinity</code>).</dd>
    </dl>
  </dd>
</dl>

### Inserting an item

```erlang
%% Insert an item at the rear
shq:in(ServerRef, Item).
shq:in(ServerRef, Item, Timeout).

%% Insert an item at the front
shq:in_r(ServerRef, Item).
shq:in_r(ServerRef, Item, Timeout).
```

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>A server reference as used with <code>gen_server</code>.</dd>
  <dt><code>Item</code></dt>
  <dd>The item to insert.</dd>
  <dt><code>Timeout</code></dt>
  <dd>A timeout to wait for a slot to become available if the queue is full (default: <code>0</code>).</dd>
</dl>

The return value is either the atom `ok` if the item was queued, the atom `full` if the queue was
full, or the atom `closed` if the queue was not accepting new items at the time of the call.

When these functions return `full` or `closed`, it is guaranteed that the given item was not inserted,
and will not be inserted later without calling this function again.

### Retrieving an item

```erlang
%% Retrieving an item from the front
shq:out(ServerRef).
shq:out(ServerRef, Timeout).

%% Retrieving an item from the rear
shq:out_r(ServerRef).
shq:out_r(ServerRef, Timeout).
```

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>See "Inserting".</dd>
  <dt><code>Timeout</code></dt>
  <dd>A timeout to wait for an item to become available if the queue is empty (default: <code>0</code>).</dd>
</dl>

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

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>See "Inserting".</dd>
</dl>

Peeking is the same as retrieving, but without removing the item from the queue.

The operation will always return instantly, ie there is no way to wait for an item to become available
if the queue is empty.

### Draining a queue

```erlang
%% Draining a quee from the front
shq:drain(ServerRef).

%% Draining a queue from the rear
shq:drain_r(ServerRef).
```

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>See "Inserting".</dd>
</dl>

The return value is a list of all items that were in the queue at the time when the call was made,
including the ones that were waiting for insertion.

### Retrieving queue size

```erlang
shq:size(ServerRef).
```

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>See "Inserting".</dd>
</dl>

Retrieves the number of items in the queue.

The return value is a non-negative integer.

### Closing and Reopening a queue

```erlang
%% Closing a queue
ok=shq:close(ServerRef).

%% Reopening a queue
ok=shq:open(ServerRef).
```

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>See "Inserting".</dd>
</dl>

When a queue is closed, all subsequent insert operations will be rejected and return `closed`
until it is reopened.

The queue status can be queried via `shq:status(ServerRef)`.

### Stopping a queue

```erlang
ok=shq:stop(ServerRef).
```

<dl>
  <dt><code>ServerRef</code></dt>
  <dd>See "Inserting".</dd>
</dl>

All items left will be lost when a queue is stopped.


## Notes

As `shq` queues are backed by `ets` tables of type `set`, the number of items in a queue does
not affect its performance. However, as `ets` tables live in memory, `shq` queues are not
suitable for storing vast numbers of large items. It is also noteworthy that items waiting to
be inserted into a queue that is currently full will still be stored in the backing table,
albeit temporarily, and thus still against available memory.


## Authors

* Maria Scott (Maria-12648430)
* Jan Uhlig (juhlig)
