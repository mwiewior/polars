use std::sync::Arc;
use arrow::legacy::error::PolarsResult;
use polars_core::prelude::SchemaRef;
use polars_plan::plans::{AnonymousScan, AnonymousScanOptions, ScanSources};
use polars_plan::prelude::FileScanOptions;
use crate::operators::{DataChunk, PExecutionContext, Source, SourceResult};
use polars_plan::prelude::AnonymousScanArgs;

pub struct AnonymousSource {
    options: Arc<AnonymousScanOptions>,
    file_options: FileScanOptions,
    function: Arc<dyn AnonymousScan>,
    schema_ref: SchemaRef,
    verbose: bool,
}

impl AnonymousSource {
    pub fn new(
        options: Arc<AnonymousScanOptions>,
        file_options: FileScanOptions,
        function: Arc<dyn AnonymousScan>,
        schema_ref: SchemaRef,
        verbose: bool
    ) -> Self {
        Self {
            options,
            file_options,
            function,
            schema_ref,
            verbose,

        }
    }

}

impl Source for AnonymousSource {
    fn get_batches(&mut self, context: &PExecutionContext) -> PolarsResult<SourceResult> {
        let args = AnonymousScanArgs {
            n_rows: None,
            with_columns: self.file_options.with_columns.clone(),
            schema: self.schema_ref.clone(),
            output_schema: None,
            predicate: None,
            py: None,
        };
        let res = self.function.next_batch(args);
        match res {
            Ok(Some(df)) => {
                let chunk = DataChunk::new(0,df);
                Ok(SourceResult::GotMoreData(vec![chunk]))
            }
            _ => Ok(SourceResult::Finished),
        }

    }

    fn fmt(&self) -> &str {
        "anonymous"
    }
}