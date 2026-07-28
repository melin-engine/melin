# Entry-indexed Merkle hash chain

**Status: proposal. Not implemented, not scheduled.**

A redesign of the journal hash chain from a linear hash over the entry byte
stream to a Merkle tree indexed by entry position. The motivation is
auditability — O(log N) inclusion and consistency proofs — with parallelism
as a secondary and, as analysed below, ambiguous benefit.

**Nothing here has been measured.** The performance section is an
operation-count model derived from BLAKE3's structure, not benchmark data.
It is included because it changes the recommendation, not because the
numbers are trustworthy as magnitudes. Instrument before acting.

---

## The chain today

`crates/core/journal/src/chain.rs` defines, per segment:

```text
chain(S) = BLAKE3(entry_bytes[first ..= S] || anchor)
```

A single incremental hasher absorbs the raw on-disk bytes of every entry;
the value at any sequence is produced by cloning the hasher and finalizing
with the segment anchor.

Two properties are load-bearing and constrain every alternative:

1. **Batching independence.** There is no finalize schedule, so `chain(S)`
   depends only on `(anchor, bytes)`. A primary and a replica that batch
   writes differently still agree at every sequence. `ChainCheck` frames,
   handshake validation in `replication/validate.rs`, and catchup all rest
   on this.
2. **Codec independence of recovery.** Because the chain covers raw bytes
   including the CRC trailer, `SegmentChain::rebuild_from_file` recomputes
   a range without decoding entries. `open_append` and offline audit
   tooling use this.

What the current design does *not* give: any way to verify a single entry
without rehashing every preceding byte from the segment anchor.

## Design C

```text
leaf(i)   = BLAKE3(0x00 || entry_bytes[i])
node(l,r) = BLAKE3(0x01 || l || r)
chain(S)  = BLAKE3(MTH(leaves[first ..= S]) || anchor)
```

`MTH` is the RFC 6962 Merkle Tree Head over exactly `S - first + 1` leaves.
The `0x00`/`0x01` prefixes are domain separation between leaf and interior
nodes; omitting them admits second-preimage attacks. The current linear
chain does not need them, so this is new correctness surface.

**Batching independence survives** because the tree shape is a function of
the leaf count and nothing else. How leaf computation is grouped — eight at
a time, one at a time, differently on primary and replica — cannot change
the result. Grouping becomes an implementation detail rather than part of
the definition. This is the same reason BLAKE3 is itself slicing-independent
today, and it is the property that distinguishes C from B below.

**Incremental state** is the Merkle-mountain-range peaks stack: O(log N)
hashes, the same shape and cost as BLAKE3's CV stack. `value()` stays a
cheap clone-and-fold, so the "no in-stream finalize points" property is
preserved intact.

**Parallelism** goes to the leaves, which are independent and therefore
hash 8 (AVX2) or 16 (AVX-512) at a time. Interior merges are independent
within a level and equally batchable, and the merge pattern is fixed by
leaf count, so it stays deterministic.

**Proofs** are the actual payoff. An inclusion proof shows a given entry
sits at a given sequence in ~log₂(N) hashes rather than the whole journal;
a consistency proof shows the journal at T₂ is an append-only extension of
T₁, i.e. that history was not rewritten. For applications facing regulatory
audit this is a product capability, not an optimisation.

## Performance

Counting BLAKE3 compression invocations, split into serial depth
(dependency-chained) and parallel width (SIMD-fillable). Assumptions: AVX2
(`simd_degree` 8), `CHUNK_LEN` 1024, entries of ~90 bytes (`sector_writer.rs`
documents ~81–101), N entries per flush batch.

Two structural facts drive the result:

- **Tree hashing costs roughly 2N compressions where linear hashing costs
  ~1.4N.** A tree over N leaves is N leaf hashes plus N−1 interior nodes.
  BLAKE3 already is a Merkle tree, but its leaves are 1 KiB — sixteen
  compressions each — so its interior overhead is ~1/16 and invisible.
  Design C's leaves are single entries, around two compressions each, so
  interior overhead approaches 100%.
- **Linear hashing cannot fill SIMD lanes below 2 KiB.** Within a 1 KiB
  chunk the sixteen block compressions are serially chained; only whole
  chunks parallelise. At ~90-byte entries that means no parallelism under
  ~23 entries per batch and no full width under ~91.

Effective serial-equivalent compressions:

| Entries / batch | Linear (today) | Design C |
| --- | --- | --- |
| 11 (~1 KiB) | ~16 | ~7 |
| 23 (~2 KiB) | ~16 | ~11 |
| 91 (~8 KiB) | ~16 | ~35 |
| 182 (~16 KiB) | ~32 | ~70 |

C wins below roughly 30–40 entries per batch and loses by a similar factor
above it. **It is not a general performance win, and adopting it for speed
without knowing the batch-size distribution risks a regression.** The
coarsening fix — defining a leaf as a deterministic run of K entries, which
keeps batching independence since K is a format constant — reduces interior
overhead to 1/K but then needs K×8 entries to fill lanes, converging back to
today's behaviour. The tension is fundamental: fine leaves parallelise early
but cost 2×; coarse leaves are cheap but need volume.

Measuring the histogram of `batch_len` at each flush is the cheap
prerequisite for any decision here.

## Alternatives considered: A and B

Two weaker variants were evaluated first. **A** keeps per-entry leaves but
folds them with a serial chain, `chain(S) = BLAKE3(chain(S-1) || leaf(S))`.
It preserves batching independence and needs no protocol change, but the
fold is one compression per entry with each step waiting on the previous, so
its serial depth is N where C's is log₂(N) — by the model above it is worse
than doing nothing at large batch sizes, and it is dominated by C at every
size. **B** builds a Merkle root per *flush batch* and chains the roots.
Performance is essentially identical to C, but defining the tree over flush
boundaries destroys batching independence: a primary and replica that batch
differently would disagree at every sequence. Restoring agreement would mean
journaling batch boundaries into the on-disk format, making flush cadence —
a performance tuning knob — part of format compatibility, and it would reach
into `ChainCheck`, handshake validation, and catchup. C obtains B's
performance while keeping the property, so neither A nor B is worth
pursuing.

## Orthogonal: offloading the chain to a background thread

Independent of any redesign, the chain could be computed off the journal
stage entirely, and this is likely the cheaper first move. The chain value
is never read on the ack path — `publish_fsync_state` is a non-blocking
SeqLock store and `evaluate_durability` reads journal and replica cursors
only — so its consumers (shadow snapshots, segment rotation, replica
handshakes, periodic `ChainCheck`) all run orders of magnitude less often
than entries are produced. A hasher thread fed entry bytes over an SPSC
queue need only keep up on average. The decisive advantage over A, B and C
is that **chain values do not change**: no format migration, no regenerated
golden vectors, no mixed-version cluster plan, no protocol change, and no
loss of codec-independent recovery. The costs are a core — non-trivial given
CCD topology constraints, and it relocates CPU rather than reducing it — a
buffer handoff (extend the existing reclaim-on-CQE rotation to a third
buffer rather than copying), a rotation path that must drain the hasher
before anchoring a new segment, supervision so a panicking hasher halts the
pipeline, and care around the `(journal_seq, chain_hash)` pairing that
`pipeline.rs` and `validate.rs` rely on being TOCTOU-free, which would
become a self-consistent but lagging pair. Worth checking first whether the
journal stage already has an idle window during the io_uring CQE wait, which
would cost no new thread at all. The two ideas compose: if one hasher core
cannot keep up at peak rates, C's independent leaves are precisely what
allows sharding leaf hashing across several hasher threads — a better
argument for C than inline SIMD.

## Costs and open problems

**Parallel leaf hashing of variable-length entries has no clean API.**
`blake3::platform::Platform::hash_many` is public but takes const-generic
same-size inputs with manual CV, counter and flag handling. Journal entries
vary in length, so extracting the SIMD win means either vendoring a
multi-buffer BLAKE3 that handles ragged lengths or writing the lane logic by
hand — a subtle-bug surface inside an audit hash. A mitigation: because
determinism is per leaf, leaves may be grouped freely, so same-length
entries can be bucketed into `hash_many` calls with a serial fallback for
odd sizes. Applications with a fixed `AppEvent::encoded_size()` would land
nearly every entry in one bucket and need no custom SIMD.

**Recovery loses codec independence, and this is a published guarantee.**
`rebuild_from_file` would have to walk entry headers to locate leaf
boundaries instead of absorbing a raw byte range. Recovery already decodes
entries so the marginal cost there is small — but the property is documented
to customers, not merely relied on internally: `docs/journal-rotation.md`
states that a sealed segment "can even be verified without journal-aware
tooling: `BLAKE3(bytes[4096..valid_end] ‖ anchor)` must equal the
successor's anchor", and `docs/journal.md` repeats the raw-byte definition.
Design C invalidates both. Auditors who built verification around that
formula would need new tooling, so the migration is external as well as
internal and the customer docs must change in lockstep.

**Format migration.** Chain values change, so golden vectors regenerate,
snapshots carry a `chain_hash` (`transport-core/src/snapshot.rs`) whose
compatibility is affected, and mixed-version clusters need a rollout plan.

## Recommendation

Build C for the proofs, not for the speed — the performance case is
genuinely ambiguous and the migration cost is hard to justify on cycles
alone. Suggested ordering:

1. Measure whether the chain costs the journal stage anything, and capture
   the `batch_len` histogram while doing so.
2. If it does, offload hashing to a background thread. No format risk.
3. Build C only if audit proofs are wanted on their own merits, or if a
   single hasher core cannot keep up and the work needs sharding.
