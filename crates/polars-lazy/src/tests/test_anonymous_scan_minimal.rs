#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    use polars_core::prelude::*;
    use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};

    use crate::prelude::*;

    /// Simple test implementation of AnonymousScan that returns a single batch
    struct TestScan {
        data: Mutex<Option<DataFrame>>,
        schema: SchemaRef,
    }

    impl TestScan {
        fn new(df: DataFrame) -> Self {
            let schema = df.schema().clone();
            Self {
                data: Mutex::new(Some(df)),
                schema,
            }
        }
    }

    impl AnonymousScan for TestScan {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
            let mut data = self.data.lock().unwrap();
            Ok(data.take().unwrap_or_else(DataFrame::empty))
        }

        fn next_batch(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
            let mut data = self.data.lock().unwrap();
            Ok(data.take())
        }

        fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
            Ok(self.schema.clone())
        }
    }

    #[test]
    fn test_anonymous_scan_basic() -> PolarsResult<()> {
        // Create test data
        let test_df = df! {
            "a" => [1, 2, 3],
            "b" => ["x", "y", "z"]
        }?;

        let scan = Arc::new(TestScan::new(test_df.clone()));

        // Test basic execution (without streaming first)
        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .collect()?;

        assert!(result.equals(&test_df));
        Ok(())
    }

    #[test]
    #[cfg(feature = "streaming")]
    fn test_anonymous_scan_streaming_basic() -> PolarsResult<()> {
        // Create test data
        let test_df = df! {
            "a" => [1, 2, 3],
            "b" => ["x", "y", "z"]
        }?;

        let scan = Arc::new(TestScan::new(test_df.clone()));

        // Test streaming execution
        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .with_streaming(true)
            .collect()?;

        assert!(result.equals(&test_df));
        Ok(())
    }
}