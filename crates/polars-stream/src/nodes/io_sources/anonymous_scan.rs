//! Anonymous scan support for streaming engine

use std::sync::Arc;

use async_trait::async_trait;
use polars_core::frame::DataFrame;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_bail};
use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};
use polars_plan::prelude::{AnonymousScanOptions, UnifiedScanArgs};
use polars_utils::pl_str::PlSmallStr;

use crate::async_executor::{JoinHandle, TaskPriority, spawn};
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
    pub function: Arc<dyn AnonymousScan>,
    pub options: Arc<AnonymousScanOptions>,
    pub unified_scan_args: Box<UnifiedScanArgs>,
    pub schema: SchemaRef,
    pub output_schema: SchemaRef,
}

impl FileReaderBuilder for AnonymousScanReaderBuilder {
    fn reader_name(&self) -> &str {
        &self.options.fmt_str
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

        Box::new(AnonymousScanBatchReader {
            function: self.function.clone(),
            scan_args: AnonymousScanArgs {
                n_rows: self.unified_scan_args.pre_slice.as_ref().map(|slice| {
                    match slice {
                        polars_utils::slice_enum::Slice::Positive { len, .. } => *len,
                        polars_utils::slice_enum::Slice::Negative { .. } => {
                            // For negative slices, we can't determine n_rows upfront
                            // Let the scan handle the full data and slice afterward
                            usize::MAX
                        }
                    }
                }),
                with_columns: self.unified_scan_args.projection.clone(),
                schema: self.schema.clone(),
                output_schema: Some(self.output_schema.clone()),
                predicate: None, // TODO: Fix predicate handling
            },
            exhausted: false,
        })
    }
}

impl std::fmt::Debug for AnonymousScanReaderBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnonymousScanReaderBuilder")
            .field("options", &self.options)
            .finish()
    }
}

pub struct AnonymousScanBatchReader {
    pub function: Arc<dyn AnonymousScan>,
    pub scan_args: AnonymousScanArgs,
    pub exhausted: bool,
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
                    file_schema_tx: _,
                    n_rows_in_file_tx: _,
                    row_position_on_end_tx: _,
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

        let function = self.function.clone();
        let scan_args = self.scan_args.clone();

        let (mut morsel_sender, morsel_rx) = FileReaderOutputSend::new_serial();

        let handle = spawn(TaskPriority::Low, async move {
            let mut seq: u64 = 0;
            let source_token = SourceToken::new();

            // Get data using next_batch method
            loop {
                match function.next_batch(scan_args.clone()) {
                    Ok(Some(df)) => {
                        if df.is_empty() {
                            break;
                        }

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
                        break;
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            Ok(())
        });

        Ok((morsel_rx, handle))
    }
}