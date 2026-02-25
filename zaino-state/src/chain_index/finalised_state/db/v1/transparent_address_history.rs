//! ZainoDB::V1 transparent address history indexing functionality.

use super::*;

/// [`TransparentHistExt`] capability implementation for [`DbV1`].
///
/// Provides address history queries built over the LMDB `DUP_SORT`/`DUP_FIXED` address-history
/// database.
#[async_trait]
impl TransparentHistExt for DbV1 {
    async fn addr_records(
        &self,
        addr_script: AddrScript,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        self.addr_records(addr_script).await
    }

    async fn addr_and_index_records(
        &self,
        addr_script: AddrScript,
        tx_location: TxLocation,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        self.addr_and_index_records(addr_script, tx_location).await
    }

    async fn addr_tx_locations_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<TxLocation>>, FinalisedStateError> {
        self.addr_tx_locations_by_range(addr_script, start_height, end_height)
            .await
    }

    async fn addr_utxos_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<(TxLocation, u16, u64)>>, FinalisedStateError> {
        self.addr_utxos_by_range(addr_script, start_height, end_height)
            .await
    }

    async fn addr_balance_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        self.addr_balance_by_range(addr_script, start_height, end_height)
            .await
    }

    async fn get_outpoint_spender(
        &self,
        outpoint: Outpoint,
    ) -> Result<Option<TxLocation>, FinalisedStateError> {
        self.get_outpoint_spender(outpoint).await
    }

    async fn get_outpoint_spenders(
        &self,
        outpoints: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxLocation>>, FinalisedStateError> {
        self.get_outpoint_spenders(outpoints).await
    }
}

impl DbV1 {
    // *** Public fetcher methods - Used by DbReader ***

    /// Fetch all address history records for a given transparent address.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more valid records exist,
    /// - `Ok(None)` if no records exist (not an error),
    /// - `Err(...)` if any decoding or DB error occurs.
    async fn addr_records(
        &self,
        addr_script: AddrScript,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            let mut raw_records = Vec::new();

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN {
                    continue;
                }
                if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                    continue;
                }
                raw_records.push(val.to_vec());
            }

            if raw_records.is_empty() {
                return Ok(None);
            }

            let mut records = Vec::with_capacity(raw_records.len());
            for val in raw_records {
                let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(&val).map_err(|e| {
                    FinalisedStateError::Custom(format!("addrhist decode error: {e}"))
                })?;
                records.push(entry.item);
            }

            Ok(Some(records))
        })
    }

    /// Fetch all address history records for a given address and TxLocation.
    ///
    /// Returns:
    /// - `Ok(Some(records))` if one or more matching records are found at that index,
    /// - `Ok(None)` if no matching records exist (not an error),
    /// - `Err(...)` on decode or DB failure.
    async fn addr_and_index_records(
        &self,
        addr_script: AddrScript,
        tx_location: TxLocation,
    ) -> Result<Option<Vec<AddrEventBytes>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        let rec_results = tokio::task::block_in_place(|| {
            self.addr_hist_records_by_addr_and_index_blocking(&addr_bytes, tx_location)
        });

        let raw_records = match rec_results {
            Ok(records) => records,
            Err(FinalisedStateError::LmdbError(lmdb::Error::NotFound)) => return Ok(None),
            Err(e) => return Err(e),
        };

        if raw_records.is_empty() {
            return Ok(None);
        }

        let mut records = Vec::with_capacity(raw_records.len());

        for val in raw_records {
            let entry = StoredEntryFixed::<AddrEventBytes>::from_bytes(&val)
                .map_err(|e| FinalisedStateError::Custom(format!("addrhist decode error: {e}")))?;
            records.push(entry.item);
        }

        Ok(Some(records))
    }

    /// Fetch all distinct `TxLocation` values for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more matching records are found,
    /// - `Ok(None)` if no matches found (not an error),
    /// - `Err(...)` on decode or DB failure.
    async fn addr_tx_locations_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<TxLocation>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };
            let mut set: HashSet<TxLocation> = HashSet::new();

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN
                    || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                {
                    continue;
                }

                // Parse the tx_location out of val:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let block_height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
                if block_height < start_height.0 || block_height > end_height.0 {
                    continue;
                }

                let tx_index = u16::from_be_bytes([val[6], val[7]]);
                set.insert(TxLocation::new(block_height, tx_index));
            }
            let mut indices: Vec<_> = set.into_iter().collect();
            indices.sort_by_key(|txi| (txi.block_height(), txi.tx_index()));

            if indices.is_empty() {
                Ok(None)
            } else {
                Ok(Some(indices))
            }
        })
    }

    /// Fetch all UTXOs (unspent mined outputs) for `addr_script` within the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Each entry is `(TxLocation, vout, value)`.
    ///
    /// Returns:
    /// - `Ok(Some(vec))` if one or more UTXOs are found,
    /// - `Ok(None)` if none found (not an error),
    /// - `Err(...)` on decode or DB failure.
    async fn addr_utxos_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<Option<Vec<(TxLocation, u16, u64)>>, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };
            let mut utxos = Vec::new();

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => return Ok(None),
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN
                    || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                {
                    continue;
                }

                // Parse the tx_location out of val:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let block_height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
                if block_height < start_height.0 || block_height > end_height.0 {
                    continue;
                }

                let flags = val[10];
                if (flags & AddrEventBytes::FLAG_MINED == 0)
                    || (flags & AddrEventBytes::FLAG_SPENT != 0)
                {
                    continue;
                }

                let tx_index = u16::from_be_bytes([val[6], val[7]]);
                let vout = u16::from_be_bytes([val[8], val[9]]);
                let value = u64::from_le_bytes([
                    val[11], val[12], val[13], val[14], val[15], val[16], val[17], val[18],
                ]);

                utxos.push((TxLocation::new(block_height, tx_index), vout, value));
            }

            if utxos.is_empty() {
                Ok(None)
            } else {
                Ok(Some(utxos))
            }
        })
    }

    /// Computes the transparent balance change for `addr_script` over the
    /// height range `[start_height, end_height]` (inclusive).
    ///
    /// Includes:
    /// - `+value` for mined outputs
    /// - `−value` for spent inputs
    ///
    /// Returns the signed net value as `i64`, or error on failure.
    async fn addr_balance_by_range(
        &self,
        addr_script: AddrScript,
        start_height: Height,
        end_height: Height,
    ) -> Result<i64, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;

        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            let mut cursor = match txn.open_ro_cursor(self.address_history) {
                Ok(cursor) => cursor,
                Err(lmdb::Error::NotFound) => {
                    return Err(FinalisedStateError::DataUnavailable(
                        "no data for address".to_string(),
                    ))
                }
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            let mut balance: i64 = 0;

            let iter = match cursor.iter_dup_of(&addr_bytes) {
                Ok(iter) => iter,
                Err(lmdb::Error::NotFound) => {
                    return Err(FinalisedStateError::DataUnavailable(
                        "no data for address".to_string(),
                    ))
                }
                Err(e) => return Err(FinalisedStateError::LmdbError(e)),
            };

            for (key, val) in iter {
                if key.len() != AddrScript::VERSIONED_LEN
                    || val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                {
                    continue;
                }

                // Parse the tx_location out of val:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let height = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
                if height < start_height.0 || height > end_height.0 {
                    continue;
                }

                let flags = val[10];
                let value = u64::from_le_bytes([
                    val[11], val[12], val[13], val[14], val[15], val[16], val[17], val[18],
                ]) as i64;

                if flags & AddrEventBytes::FLAG_IS_INPUT != 0 {
                    balance -= value;
                } else if flags & AddrEventBytes::FLAG_MINED != 0 {
                    balance += value;
                }
            }

            Ok(balance)
        })
    }

    /// Fetch the `TxLocation` that spent a given outpoint, if any.
    ///
    /// Returns:
    /// - `Ok(Some(TxLocation))` if the outpoint is spent.
    /// - `Ok(None)` if no entry exists (not spent or not known).
    /// - `Err(...)` on deserialization or DB error.
    async fn get_outpoint_spender(
        &self,
        outpoint: Outpoint,
    ) -> Result<Option<TxLocation>, FinalisedStateError> {
        let key = outpoint.to_bytes()?;
        let txn = self.env.begin_ro_txn()?;

        tokio::task::block_in_place(|| match txn.get(self.spent, &key) {
            Ok(bytes) => {
                let entry = StoredEntryFixed::<TxLocation>::from_bytes(bytes).map_err(|e| {
                    FinalisedStateError::Custom(format!("spent entry decode error: {e}"))
                })?;
                Ok(Some(entry.item))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(FinalisedStateError::LmdbError(e)),
        })
    }

    /// Fetch the `TxLocation` entries for a batch of outpoints.
    ///
    /// For each input:
    /// - Returns `Some(TxLocation)` if spent,
    /// - `None` if not found,
    /// - or returns `Err` immediately if any DB or decode error occurs.
    async fn get_outpoint_spenders(
        &self,
        outpoints: Vec<Outpoint>,
    ) -> Result<Vec<Option<TxLocation>>, FinalisedStateError> {
        tokio::task::block_in_place(|| {
            let txn = self.env.begin_ro_txn()?;

            outpoints
                .into_iter()
                .map(|outpoint| {
                    let key = outpoint.to_bytes()?;
                    match txn.get(self.spent, &key) {
                        Ok(bytes) => {
                            let entry =
                                StoredEntryFixed::<TxLocation>::from_bytes(bytes).map_err(|e| {
                                    FinalisedStateError::Custom(format!(
                                        "spent entry decode error for {outpoint:?}: {e}"
                                    ))
                                })?;
                            Ok(Some(entry.item))
                        }
                        Err(lmdb::Error::NotFound) => Ok(None),
                        Err(e) => Err(FinalisedStateError::LmdbError(e)),
                    }
                })
                .collect()
        })
    }

    // *** Internal DB methods ***

    /// Returns all raw AddrHist records for a given AddrScript and TxLocation.
    ///
    /// Returns a Vec of serialized entries, for given addr_script and ix_index.
    ///
    /// Efficiently filters by matching block + tx index bytes in-place.
    ///
    /// WARNING: This is a blocking function and **MUST** be called within a blocking thread / task.
    pub(super) fn addr_hist_records_by_addr_and_index_blocking(
        &self,
        addr_script_bytes: &Vec<u8>,
        tx_location: TxLocation,
    ) -> Result<Vec<Vec<u8>>, FinalisedStateError> {
        let txn = self.env.begin_ro_txn()?;

        let mut cursor = txn.open_ro_cursor(self.address_history)?;
        let mut results = Vec::new();

        for (key, val) in cursor.iter_dup_of(&addr_script_bytes)? {
            if key.len() != AddrScript::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }
            if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                // TODO: Return error?
                break;
            }

            // Check tx_location match without deserializing
            // - [0] StoredEntry tag
            // - [1] record tag
            // - [2..=5] height
            // - [6..=7] tx_index
            // - [8..=9] vout
            // - [10] flags
            // - [11..=18] value
            // - [19..=50] checksum

            let block_index = u32::from_be_bytes([val[2], val[3], val[4], val[5]]);
            let tx_idx = u16::from_be_bytes([val[6], val[7]]);

            if block_index == tx_location.block_height() && tx_idx == tx_location.tx_index() {
                results.push(val.to_vec());
            }
        }

        Ok(results)
    }

    /// Inserts a mined-output record into the address‐history map.
    #[inline]
    pub(super) fn build_transaction_output_histories<'a>(
        map: &mut HashMap<AddrScript, Vec<AddrHistRecord>>,
        tx_location: TxLocation,
        outputs: impl Iterator<Item = (usize, &'a TxOutCompact)>,
    ) {
        for (output_idx, output) in outputs {
            let addr_script = AddrScript::new(*output.script_hash(), output.script_type());
            let output_record = AddrHistRecord::new(
                tx_location,
                output_idx as u16,
                output.value(),
                AddrHistRecord::FLAG_MINED,
            );
            map.entry(addr_script)
                .and_modify(|v| v.push(output_record))
                .or_insert_with(|| vec![output_record]);
        }
    }

    /// Inserts both the “spend” record and the “mined” previous‐output record
    /// (used to update the output record spent in this transaction).
    #[inline]
    #[allow(clippy::type_complexity)]
    pub(super) fn build_input_history(
        map: &mut HashMap<AddrScript, Vec<(AddrHistRecord, (AddrScript, AddrHistRecord))>>,
        input_tx_location: TxLocation,
        input_index: u16,
        input: &TxInCompact,
        prev_output: &TxOutCompact,
        prev_output_tx_location: TxLocation,
    ) {
        let addr_script = AddrScript::new(*prev_output.script_hash(), prev_output.script_type());
        let input_record = AddrHistRecord::new(
            input_tx_location,
            input_index,
            prev_output.value(),
            AddrHistRecord::FLAG_IS_INPUT,
        );
        let prev_output_record = (
            AddrScript::new(*prev_output.script_hash(), prev_output.script_type()),
            AddrHistRecord::new(
                prev_output_tx_location,
                input.prevout_index() as u16,
                prev_output.value(),
                AddrHistRecord::FLAG_MINED,
            ),
        );
        map.entry(addr_script)
            .and_modify(|v| v.push((input_record, prev_output_record)))
            .or_insert_with(|| vec![(input_record, prev_output_record)]);
    }

    /// Delete all `addrhist` duplicates for `addr_bytes` that
    ///   * belong to `block_height`, **and**
    ///   * match the requested record type(s).
    ///
    /// * `delete_inputs`  – remove records whose flag-byte contains FLAG_IS_INPUT
    /// * `delete_outputs` – remove records whose flag-byte contains FLAG_MINED
    ///
    /// `expected` is the number of records to delete;
    ///
    /// WARNING: This is a blocking function and **MUST** be called within a blocking thread / task.
    pub(super) fn delete_addrhist_dups_blocking(
        &self,
        addr_bytes: &[u8],
        block_height: Height,
        delete_inputs: bool,
        delete_outputs: bool,
        expected: usize,
    ) -> Result<(), FinalisedStateError> {
        if !delete_inputs && !delete_outputs {
            return Err(FinalisedStateError::Custom(
                "called delete_addrhist_dups with neither inputs nor outputs to delete".into(),
            ));
        }
        if expected == 0 {
            return Err(FinalisedStateError::Custom(
                "called delete_addrhist_dups with 0 expected deletes".into(),
            ));
        }

        let mut remaining = expected;
        let height_be = block_height.0.to_be_bytes();

        let mut txn = self.env.begin_rw_txn()?;
        let mut cur = txn.open_rw_cursor(self.address_history)?;

        match cur
            .get(Some(addr_bytes), None, lmdb_sys::MDB_SET_KEY)
            .and_then(|_| cur.get(None, None, lmdb_sys::MDB_LAST_DUP))
        {
            Ok((_k, mut val)) => loop {
                // Parse AddrEventBytes:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum
                if val.len() == StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN
                    && val[2..6] == height_be
                {
                    let flags = val[10];
                    let is_input = flags & AddrEventBytes::FLAG_IS_INPUT != 0;
                    let is_output = flags & AddrEventBytes::FLAG_MINED != 0;

                    if (delete_inputs && is_input) || (delete_outputs && is_output) {
                        cur.del(WriteFlags::empty())?;
                        remaining -= 1;
                        if remaining == 0 {
                            break;
                        }
                    }
                } else if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                    tracing::warn!("bad addrhist dup (len={})", val.len());
                }

                // step backwards through duplicates
                match cur.get(None, None, lmdb_sys::MDB_PREV_DUP) {
                    Ok((_k, v)) => val = v,
                    Err(lmdb::Error::NotFound) => {
                        if remaining == 0 {
                            break;
                        }
                        return Err(FinalisedStateError::Custom(format!(
                            "expected {expected} records, deleted {}",
                            expected - remaining
                        )));
                    }
                    Err(e) => return Err(FinalisedStateError::LmdbError(e)),
                }
            },
            Err(lmdb::Error::NotFound) => {
                return Err(FinalisedStateError::Custom(
                    "no addrhist record for key".into(),
                ));
            }
            Err(e) => return Err(FinalisedStateError::LmdbError(e)),
        }

        drop(cur);
        txn.commit()?;
        Ok(())
    }

    /// Mark a specific AddrHistRecord as spent in the addrhist DB.
    /// Looks up a record by script and tx_location, sets FLAG_SPENT, and updates it in place.
    ///
    /// Returns Ok(true) if a record was updated, Ok(false) if not found, or Err on DB error.
    ///
    /// WARNING: This is a blocking function and **MUST** be called within a blocking thread / task.
    pub(super) fn mark_addr_hist_record_spent_blocking(
        &self,
        addr_script: &AddrScript,
        tx_location: TxLocation,
        vout: u16,
    ) -> Result<bool, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let mut txn = self.env.begin_rw_txn()?;
        {
            let mut cur = txn.open_rw_cursor(self.address_history)?;

            for (key, val) in cur.iter_dup_of(&addr_bytes)? {
                if key.len() != AddrScript::VERSIONED_LEN {
                    // TODO: Return error?
                    break;
                }
                if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                    // TODO: Return error?
                    break;
                }
                let mut hist_record = [0u8; StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN];
                hist_record.copy_from_slice(val);

                // Parse the tx_location out of arr:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let block_height = u32::from_be_bytes([
                    hist_record[2],
                    hist_record[3],
                    hist_record[4],
                    hist_record[5],
                ]);
                let tx_idx = u16::from_be_bytes([hist_record[6], hist_record[7]]);
                let rec_vout = u16::from_be_bytes([hist_record[8], hist_record[9]]);
                let flags = hist_record[10];

                // Skip if this row is an input or already marked spent.
                if flags & AddrHistRecord::FLAG_IS_INPUT != 0
                    || flags & AddrHistRecord::FLAG_SPENT != 0
                {
                    continue;
                }

                // Match on (height, tx_location, vout).
                if block_height == tx_location.block_height()
                    && tx_idx == tx_location.tx_index()
                    && rec_vout == vout
                {
                    // Flip FLAG_SPENT.
                    hist_record[10] |= AddrHistRecord::FLAG_SPENT;

                    // Recompute checksum over entry header + payload (bytes 1‥19).
                    let checksum = StoredEntryFixed::<AddrEventBytes>::blake2b256(
                        &[&addr_bytes, &hist_record[1..19]].concat(),
                    );
                    hist_record[19..51].copy_from_slice(&checksum);

                    // Write back in place.
                    cur.put(&addr_bytes, &hist_record, WriteFlags::CURRENT)?;
                    drop(cur);
                    txn.commit()?;

                    return Ok(true);
                }
            }
        }
        txn.commit()?;
        Ok(false)
    }

    /// Mark a specific AddrHistRecord as unspent in the addrhist DB.
    /// Looks up a record by script and tx_location, sets FLAG_SPENT, and updates it in place.
    ///
    /// Returns Ok(true) if a record was updated, Ok(false) if not found, or Err on DB error.
    ///
    /// WARNING: This is a blocking function and **MUST** be called within a blocking thread / task.
    pub(super) fn mark_addr_hist_record_unspent_blocking(
        &self,
        addr_script: &AddrScript,
        tx_location: TxLocation,
        vout: u16,
    ) -> Result<bool, FinalisedStateError> {
        let addr_bytes = addr_script.to_bytes()?;
        let mut txn = self.env.begin_rw_txn()?;
        {
            let mut cur = txn.open_rw_cursor(self.address_history)?;

            for (key, val) in cur.iter_dup_of(&addr_bytes)? {
                if key.len() != AddrScript::VERSIONED_LEN {
                    // TODO: Return error?
                    break;
                }
                if val.len() != StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN {
                    // TODO: Return error?
                    break;
                }
                let mut hist_record = [0u8; StoredEntryFixed::<AddrEventBytes>::VERSIONED_LEN];
                hist_record.copy_from_slice(val);

                // Parse the tx_location out of arr:
                // - [0] StoredEntry tag
                // - [1] record tag
                // - [2..=5] height
                // - [6..=7] tx_index
                // - [8..=9] vout
                // - [10] flags
                // - [11..=18] value
                // - [19..=50] checksum

                let block_height = u32::from_be_bytes([
                    hist_record[2],
                    hist_record[3],
                    hist_record[4],
                    hist_record[5],
                ]);
                let tx_idx = u16::from_be_bytes([hist_record[6], hist_record[7]]);
                let rec_vout = u16::from_be_bytes([hist_record[8], hist_record[9]]);
                let flags = hist_record[10];

                // Skip if this row is an input or already marked unspent.
                if (flags & AddrHistRecord::FLAG_IS_INPUT) != 0
                    || (flags & AddrHistRecord::FLAG_SPENT) == 0
                {
                    continue;
                }

                // Match on (height, tx_location, vout).
                if block_height == tx_location.block_height()
                    && tx_idx == tx_location.tx_index()
                    && rec_vout == vout
                {
                    // Flip FLAG_SPENT.
                    hist_record[10] &= !AddrHistRecord::FLAG_SPENT;

                    // Recompute checksum over entry header + payload (bytes 1‥19).
                    let checksum = StoredEntryFixed::<AddrEventBytes>::blake2b256(
                        &[&addr_bytes, &hist_record[1..19]].concat(),
                    );
                    hist_record[19..51].copy_from_slice(&checksum);

                    // Write back in place.
                    cur.put(&addr_bytes, &hist_record, WriteFlags::CURRENT)?;
                    drop(cur);
                    txn.commit()?;

                    return Ok(true);
                }
            }
        }
        txn.commit()?;
        Ok(false)
    }
}
