# `zaino-chain-store` — usage

The domain half of the finalised-state subsystem: vocabulary and ports for
everything below the reorg seam. No runtime, no storage, no `zebra-chain`, no
`tonic`. The LMDB implementation is
[`zaino-chain-store-zainodb`](../zaino-chain-store-zainodb/usage.md).

Depend on this crate to *name* what a store answers. Depend on an
implementation only if you are the one starting it.

```rust
use zaino_chain_store::{ChainStoreReader, StoredBlockRead};

/// Works against any store that can serve stored blocks, and knows nothing
/// about how one is stored.
async fn tip_block<R: StoredBlockRead>(reader: &R) -> Option<StoredBlock> {
    let tip = reader.watermark().tip?;
    reader.blocks_chunk(tip.height, tip.height).await.ok()?.pop()
}
```

## Bound on the capability, not on the store

Only `ChainStoreReader` is universal. Compact blocks, transaction positions,
spent outputs, the txout set and address history are each their own trait.

Write `R: ChainStoreReader + TransactionIndex` and you have said exactly what
you use; a store that cannot serve transaction positions fails to compile
against you rather than failing at runtime. A single fat trait would force every
implementation to stub what it cannot do, and stubs are where
`Err(Unsupported)` at 3am comes from.

Where absence *cannot* be a compile-time fact it is a runtime one:
`capabilities()` exists because a store on an older schema genuinely lacks an
index until it has migrated. That is a fact about a database, not a type.

## The watermark is the boundary, and a read past it is not a miss

`ChainStoreError::AboveWatermark` means the height is not this store's to answer.
The block very likely exists — in the chain head. Treating it as absence is the
single most likely way to serve a wrong answer through this crate.

```rust
match reader.blocks_chunk(start, end).await {
    Err(ChainStoreError::AboveWatermark { watermark, .. }) => {
        // Ask the chain head for `(watermark, end]`, then concatenate.
    }
    // ...
}
```

Pin the watermark **once** for a request that spans the seam and derive both
sides from that one value. Re-reading it between the two halves lets the store
advance in the middle and produces a gap or an overlap.

### The boundary only applies to durable answers

Read `provenance` before you read `tip`. A store whose provenance is
`Passthrough` is answering from the validator rather than from what it holds, so
its durable tip is not a limit on what it can answer, and it will not refuse
above it. That is the state a store is in for the whole of a long initial
build — precisely when a node depends on it to stay useful — so a consumer that
treated `AboveWatermark` as the only way a read can be out of range would be
right about a settled store and wrong about a building one.

What the watermark still tells you in that state is what the store *holds*,
which is what a consumer deciding whether to trust it as a durable record wants.
The two questions are different, and `provenance` is which one you are asking.

## Configuration is two halves, and the split is not arbitrary

`ChainStoreConfig` is what every store takes. An implementation pairs it with
its own type for the things a domain crate cannot name — for ZainoDB that is
`ZainoDbConfig`, carrying an LMDB budget and a `zebra-chain` network, and
nothing else:

```rust
FinalisedState::spawn(
    ChainStoreConfig::at_path("/var/lib/zaino"),
    ZainoDbConfig::new(network),
    source,
)
```

The rule for deciding which half a knob belongs in: ask whether a second
implementation would have the same question. Where the store lives, which
schema to target, and how it behaves when a build fails are the same question
for any store. A memory-map size is not.

Fields are private and read through accessors, and three of the four numeric
knobs are `NonZero` — the same shape as `MempoolConfig` and `ChainHeadConfig`.
The one that keeps its zero is `background_build_threshold`, because zero is
meaningful there (every build runs in the background); taking `NonZero` for
uniformity would have removed a real configuration.

Note what is *not* two fields: a store that holds nothing is one with no path,
not a path beside a flag. That pair used to exist, and nothing said which won
when they disagreed.

## A stored block is not a compact block

`StoredBlock.transactions` holds `StoredTx`, not `PreIndexCompactTx`: the
compact transaction *plus* the per-pool value balances an index persists beside
it. The compact protocol has no value balance, correctly — a wallet does not
need one — but a store does, and `StoredBlock` is the shape that crosses the
write boundary as well as the read one.

That is the rule to keep when adding a field here: whatever
`ChainStoreFreezeSink::freeze` needs in order to write a block must be
expressible in what `StoredBlockRead` yields, or a block read out of one store
and frozen into another loses it silently. Both directions decode, both hash,
and only the rows differ. The backend's port suite checks exactly this by
reading a chain out of one store and freezing it into an empty one.

## Chunks, not blocks

There is no `get_block(height)`, and that is deliberate. A single block is
`blocks_chunk(h, h)`. Naming a point read invites the pattern this port
replaced: one `begin_ro_txn` per height across a range, plus one channel send
per block.

Use `blocks_chunk` when you know the range is small and you want it in hand.
Use `blocks_stream` for anything client-facing: it walks one cursor and yields
a `Vec` per read transaction, so peak memory is chunk-sized rather than
range-sized.

Ranges are **ascending and hole-intolerant**. A gap in the heights is an error,
not a skip — silently truncating a wallet's sync is worse than failing it. If
you want descending order, reverse above the port.

## `PoolFilter` goes into the read

`CompactBlockRead` is separate from `StoredBlockRead` for one reason: the filter
selects which cursors open and which row families decode. Passing it in lets a
sapling-only wallet skip orchard, ironwood and the commitment-tree rows
entirely. Filtering the result afterwards costs the decode you were avoiding, on
every block.

## The store never hands you consensus bytes

What is stored is a projection — the fields an index reads, not the bytes a
block hash commits to. A `StoredTxOut` carries a 20-byte address key and a
value; the locking script is not recoverable. `StoredAddress` can express
`NonStandard`, which `TransparentAddress` cannot, and that is why it exists.

Raw blocks and raw transactions come from the validator. No port here offers
them, so that nothing can mistake a store for a source of consensus data.

Two further limits worth knowing before you design against this crate:

- **The store cannot answer maturity questions.** Coinbase is special-cased on
  inputs — null prevouts are filtered — and no stored output carries a coinbase
  flag.
- **The address index and the txout set disagree about which outputs exist,
  by design.** The address index keys every output, including non-standard
  scripts; the txout set excludes unspendable ones per `is_unspendable`. Each
  port states which semantics it exposes; do not assume they agree.

## `txout_set` is a partial fold, not an answer

`TxOutSetIndex::txout_set` returns an accumulator over the finalised set. It is
completed with the chain head's blocks by whatever merges the two. Serving it
directly as `gettxoutsetinfo` reports the chain as of the watermark, which is
not the chain.

The commitment lives in this crate rather than in an implementation because two
stores disagreeing about it would not fail — they would quietly mean different
things by the same number.

## Do not build on `StoreCapabilities`

It is interim wiring: the backend's internal routing model, one bit per storage
trait, surfaced so `ChainIndex` keeps working until the chain view lands. It is
storage-shaped where the layer above needs "what is answerable to height H" per
*domain* capability. Its replacement is planned; adding a consumer adds work to
that replacement.
