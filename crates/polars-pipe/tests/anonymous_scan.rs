
use std::sync::Arc;
use polars_core::df;
use polars_core::prelude::{AnyValue, DataFrame, PolarsResult, Schema, SchemaRef};
use polars_pipe::executors::sources::anonymous::AnonymousSource;
use polars_pipe::operators::{PExecutionContext, Source, SourceResult};
use polars_plan::dsl::AnonymousScanOptions;
use polars_plan::plans::AnonymousScan;
use polars_plan::prelude::AnonymousScanArgs;

struct MockScan;

impl AnonymousScan for MockScan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        df! {
            "a" => &[1, 2, 3],
            "b" => &[4, 5, 6],
        }
    }

    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(Arc::new(df! {
            "a" => &[1, 2, 3],
            "b" => &[4, 5, 6],
        }.unwrap().schema().clone()))
    }
}

#[test]
fn test_anonymous_source() {
    let options = Arc::new(AnonymousScanOptions {
        skip_rows: None,
        fmt_str: "",
    });
    let function = Arc::new(MockScan);
    let schema = function.schema(None).unwrap();
    let mut source = AnonymousSource::new(options, function, schema, false);
    use polars_expr::state::ExecutionState;
    let context = PExecutionContext::new(ExecutionState::new(), false);
    let result = source.get_batches(&context).unwrap();

    match result {
        SourceResult::GotMoreData(chunks) => {
            assert_eq!(chunks.len(), 1);
            let df = chunks[0].data.clone();
            let expected = df! {
                "a" => &[1, 2, 3],
                "b" => &[4, 5, 6],
            }
            .unwrap();
            assert!(df.equals(&expected));
        }
        _ => panic!("Expected GotMoreData"),
    }
}
