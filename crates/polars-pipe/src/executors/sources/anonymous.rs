use std::sync::Arc;
use arrow::legacy::error::PolarsResult;
use polars_core::prelude::SchemaRef;
use polars_plan::dsl::AnonymousScanOptions;
use polars_plan::plans::AnonymousScan;
use crate::operators::{DataChunk, PExecutionContext, Source, SourceResult};
use polars_plan::prelude::AnonymousScanArgs;

pub struct AnonymousSource {
    _options: Arc<AnonymousScanOptions>,
    function: Arc<dyn AnonymousScan>,
    schema_ref: SchemaRef,
    _verbose: bool,
}

impl AnonymousSource {
    pub fn new(
        options: Arc<AnonymousScanOptions>,
        function: Arc<dyn AnonymousScan>,
        schema_ref: SchemaRef,
        verbose: bool,
    ) -> Self {
        Self {
            _options: options,
            function,
            schema_ref,
            _verbose: verbose,
        }
    }
}

impl Source for AnonymousSource {
    fn get_batches(&mut self, _context: &PExecutionContext) -> PolarsResult<SourceResult> {
        let args = AnonymousScanArgs {
            n_rows: None,
            with_columns: None,
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