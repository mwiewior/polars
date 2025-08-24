//! Anonymous scan support for streaming engine

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use polars_core::frame::DataFrame;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_bail, polars_err};
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};
use polars_plan::prelude::{AnonymousScanOptions, UnifiedScanArgs};
use polars_utils::pl_str::PlSmallStr;
use polars_utils::IdxSize;

use crate::async_executor::{JoinHandle, TaskPriority, spawn};
use crate::execute::StreamingExecutionState;
use crate::morsel::{Morsel, MorselSeq, SourceToken};
use crate::nodes::io_sources::multi_file_reader::reader_interface::output::{
    FileReaderOutputRecv, FileReaderOutputSend,
};
use crate::nodes::io_sources::multi_file_reader::reader_interface::{
    BeginReadArgs, FileReader, FileReaderCallbacks,
};
use crate::nodes::io_sources::multi_file_reader::reader_interface::builder::FileReaderBuilder;
use crate::nodes::io_sources::multi_file_reader::reader_interface::capabilities::ReaderCapabilities;

pub struct AnonymousScanReaderBuilder {
    pub name: PlSmallStr,
    pub reader: Mutex<Option<AnonymousScanBatchReader>>,
}

impl FileReaderBuilder for AnonymousScanReaderBuilder {
    fn reader_name(&self) -> &str {
        &self.name
    }

    fn reader_capabilities(&self) -> ReaderCapabilities {
        // Anonymous scans don't support the standard file-based capabilities
        ReaderCapabilities::empty()
    }

    fn build_file_reader(
        &self,
        _source: polars_plan::prelude::ScanSource,
        _cloud_options: Option<Arc<polars_io::cloud::CloudOptions>>,
        scan_source_idx: usize,
    ) -> Box<dyn FileReader> {
        assert_eq!(scan_source_idx, 0, "AnonymousScan should only have one source");

        Box::new(
            self.reader
                .try_lock()
                .unwrap()
                .take()
                .expect("AnonymousScanReaderBuilder called more than once"),
        ) as Box<dyn FileReader>
    }
}

impl std::fmt::Debug for AnonymousScanReaderBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnonymousScanReaderBuilder")
            .field("name", &self.name)
            .finish()
    }
}

pub struct AnonymousScanBatchReader {
    pub name: PlSmallStr,
    pub function: Arc<dyn AnonymousScan>,
    pub scan_args: AnonymousScanArgs,
    pub output_schema: Option<SchemaRef>,
    pub exhausted: bool,
    pub verbose: bool,
}

impl AnonymousScanBatchReader {
    pub fn new(
        function: Arc<dyn AnonymousScan>,
        options: Arc<AnonymousScanOptions>,
        unified_scan_args: Box<UnifiedScanArgs>,
        schema: SchemaRef,
        predicate: Option<polars_plan::plans::expr_ir::ExprIR>,
        output_schema: &SchemaRef,
    ) -> Self {
        // Convert streaming arguments to AnonymousScanArgs format
        let scan_args = AnonymousScanArgs {
            n_rows: unified_scan_args.pre_slice.as_ref().map(|slice| {
                match slice {
                    polars_utils::slice_enum::Slice::Positive { len, .. } => *len,
                    polars_utils::slice_enum::Slice::Negative { .. } => {
                        // For negative slices, we can't determine n_rows upfront
                        // Let the scan handle the full data and slice afterward
                        usize::MAX
                    }
                }
            }),
            with_columns: unified_scan_args.projection.clone(),
            schema,
            output_schema: Some(output_schema.clone()),
            predicate: if function.allows_predicate_pushdown() {
                predicate.map(|p| p.node())
            } else {
                None
            },
        };

        Self {
            name: options.fmt_str.into(),
            function,
            scan_args,
            output_schema: Some(output_schema),
            exhausted: false,
            verbose: false,
        }
    }

    fn get_next_batch(&mut self, _state: &StreamingExecutionState) -> PolarsResult<Option<DataFrame>> {
        if self.exhausted {
            return Ok(None);
        }

        match self.function.next_batch(self.scan_args.clone()) {
            Ok(Some(df)) => {
                if df.is_empty() {
                    self.exhausted = true;
                    Ok(None)
                } else {
                    Ok(Some(df))
                }
            }
            Ok(None) => {
                self.exhausted = true;
                Ok(None)
            }
            Err(e) => {
                self.exhausted = true;
                Err(e)
            }
        }
    }

    fn infer_schema(&mut self, _state: &StreamingExecutionState) -> PolarsResult<SchemaRef> {
        if let Some(schema) = &self.output_schema {
            return Ok(schema.clone());
        }

        // Try to get the first batch to infer schema
        match self.get_next_batch(_state)? {
            Some(df) => {
                let schema = df.schema();
                self.output_schema = Some(schema.clone());
                
                // Reset state since we consumed a batch for schema inference
                // This is a limitation - we'd need to buffer the first batch
                // For now, we'll recreate the scan args and let the next call handle it
                self.exhausted = false;
                
                Ok(schema.clone())
            }
            None => {
                // Empty scan, return the input schema
                Ok(self.scan_args.schema.clone())
            }
        }
    }
}

#[async_trait]
impl FileReader for AnonymousScanBatchReader {
    async fn initialize(&mut self) -> PolarsResult<()> {
        Ok(())
    }

    fn begin_read(
        &mut self,
        args: BeginReadArgs,
    ) -> PolarsResult<(FileReaderOutputRecv, JoinHandle<PolarsResult<()>>)> {
        let BeginReadArgs {
            projected_schema: _,
            row_index,
            pre_slice,
            predicate,
            cast_columns_policy: _,
            num_pipelines: _,
            callbacks:
                FileReaderCallbacks {
                    file_schema_tx,
                    n_rows_in_file_tx,
                    row_position_on_end_tx,
                },
        } = args;

        // Validate unsupported features
        if row_index.is_some() {
            polars_bail!(InvalidOperation: "row_index not supported for AnonymousScan in streaming mode");
        }
        if pre_slice.is_some() {
            polars_bail!(InvalidOperation: "pre_slice should be handled at the scan level, not here");
        }
        if predicate.is_some() && !self.function.allows_predicate_pushdown() {
            polars_bail!(InvalidOperation: "predicate pushdown not supported by this AnonymousScan");
        }

        // Send file schema first
        if let Some(mut file_schema_tx) = file_schema_tx {
            let exec_state = StreamingExecutionState::default();
            let schema = self.infer_schema(&exec_state)?;
            _ = file_schema_tx.try_send(schema);
        }

        let mut function = self.function.clone();
        let mut scan_args = self.scan_args.clone();
        let verbose = self.verbose;
        let name = self.name.clone();

        // Apply any additional predicate from BeginReadArgs
        if let Some(predicate_expr) = predicate {
            if function.allows_predicate_pushdown() {
                scan_args.predicate = Some(predicate_expr.node());
            }
        }

        if verbose {
            eprintln!("[AnonymousScanBatchReader]: name: {}", name);
        }

        let (mut morsel_sender, morsel_rx) = FileReaderOutputSend::new_serial();

        let handle = spawn(TaskPriority::Low, async move {
            let mut seq: u64 = 0;
            let source_token = SourceToken::new();
            let mut n_rows_seen: usize = 0;
            let mut exhausted = false;

            while !exhausted {
                match function.next_batch(scan_args.clone()) {
                    Ok(Some(df)) => {
                        if df.is_empty() {
                            exhausted = true;
                            break;
                        }
                        
                        n_rows_seen = n_rows_seen.saturating_add(df.height());

                        if morsel_sender
                            .send_morsel(Morsel::new(df, MorselSeq::new(seq), source_token.clone()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        seq = seq.saturating_add(1);
                    }
                    Ok(None) => {
                        exhausted = true;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            // Send row position at end
            if let Some(mut row_position_on_end_tx) = row_position_on_end_tx {
                let n_rows_seen = IdxSize::try_from(n_rows_seen)
                    .map_err(|_| polars_err!(ComputeError: "row count overflow in anonymous scan"))?;
                _ = row_position_on_end_tx.try_send(n_rows_seen);
            }

            // Send total row count if needed
            if let Some(mut n_rows_in_file_tx) = n_rows_in_file_tx {
                if verbose {
                    eprintln!("[AnonymousScanBatchReader]: computing full row count");
                }

                // We need to continue scanning to get the full count
                while let Ok(Some(df)) = function.next_batch(scan_args.clone()) {
                    if df.is_empty() {
                        break;
                    }
                    n_rows_seen = n_rows_seen.saturating_add(df.height());
                }

                let n_rows_seen = IdxSize::try_from(n_rows_seen)
                    .map_err(|_| polars_err!(ComputeError: "row count overflow in anonymous scan"))?;
                _ = n_rows_in_file_tx.try_send(n_rows_seen);
            }

            Ok(())
        });

        Ok((morsel_rx, handle))
    }
}