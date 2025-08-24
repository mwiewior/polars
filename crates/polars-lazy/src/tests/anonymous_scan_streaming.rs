#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    use polars_core::prelude::*;
    use polars_plan::plans::{AnonymousScan, AnonymousScanArgs};

    use crate::prelude::*;

    /// Simple test implementation of AnonymousScan that returns batches of data
    struct TestBatchScan {
        batches: Mutex<Vec<DataFrame>>,
        schema: SchemaRef,
        current_batch: Mutex<usize>,
        allows_predicate: bool,
        allows_projection: bool,
    }

    impl TestBatchScan {
        fn new(batches: Vec<DataFrame>) -> Self {
            let schema = if batches.is_empty() {
                Arc::new(Schema::default())
            } else {
                batches[0].schema()
            };

            Self {
                batches: Mutex::new(batches),
                schema,
                current_batch: Mutex::new(0),
                allows_predicate: false,
                allows_projection: false,
            }
        }

        fn with_pushdown(mut self, predicate: bool, projection: bool) -> Self {
            self.allows_predicate = predicate;
            self.allows_projection = projection;
            self
        }
    }

    impl AnonymousScan for TestBatchScan {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
            let batches = self.batches.lock().unwrap();
            if batches.is_empty() {
                return Ok(DataFrame::empty());
            }

            // For non-streaming scan, concatenate all batches
            let mut result = batches[0].clone();
            for batch in batches.iter().skip(1) {
                result.vstack_mut(batch)?;
            }
            Ok(result)
        }

        fn next_batch(&self, scan_opts: AnonymousScanArgs) -> PolarsResult<Option<DataFrame>> {
            let batches = self.batches.lock().unwrap();
            let mut current = self.current_batch.lock().unwrap();

            if *current >= batches.len() {
                return Ok(None);
            }

            let mut df = batches[*current].clone();
            *current += 1;

            // Apply column selection if provided
            if let Some(columns) = &scan_opts.with_columns {
                let column_names: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
                df = df.select(&column_names)?;
            }

            // Apply row limit if provided
            if let Some(n_rows) = scan_opts.n_rows {
                if n_rows < df.height() {
                    df = df.head(Some(n_rows));
                }
            }

            Ok(Some(df))
        }

        fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
            Ok(self.schema.clone())
        }

        fn allows_predicate_pushdown(&self) -> bool {
            self.allows_predicate
        }

        fn allows_projection_pushdown(&self) -> bool {
            self.allows_projection
        }
    }

    /// Single batch test implementation
    struct TestSingleScan {
        data: Mutex<Option<DataFrame>>,
        schema: SchemaRef,
    }

    impl TestSingleScan {
        fn new(df: DataFrame) -> Self {
            let schema = df.schema();
            Self {
                data: Mutex::new(Some(df)),
                schema,
            }
        }
    }

    impl AnonymousScan for TestSingleScan {
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
    fn test_anonymous_scan_streaming_basic() -> PolarsResult<()> {
        // Create test data
        let df1 = df! {
            "a" => [1, 2, 3],
            "b" => ["x", "y", "z"]
        }?;
        let df2 = df! {
            "a" => [4, 5, 6], 
            "b" => ["p", "q", "r"]
        }?;

        let scan = Arc::new(TestBatchScan::new(vec![df1, df2]));

        // Test streaming execution
        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .with_streaming(true)
            .collect()?;

        let expected = df! {
            "a" => [1, 2, 3, 4, 5, 6],
            "b" => ["x", "y", "z", "p", "q", "r"]
        }?;

        assert!(result.equals(&expected));
        Ok(())
    }

    #[test] 
    fn test_anonymous_scan_streaming_with_projection() -> PolarsResult<()> {
        let df1 = df! {
            "a" => [1, 2, 3],
            "b" => ["x", "y", "z"],
            "c" => [10, 20, 30]
        }?;
        let df2 = df! {
            "a" => [4, 5, 6],
            "b" => ["p", "q", "r"],
            "c" => [40, 50, 60]
        }?;

        let scan = Arc::new(TestBatchScan::new(vec![df1, df2]).with_pushdown(false, true));

        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .select([col("a"), col("c")])
            .with_streaming(true)
            .collect()?;

        let expected = df! {
            "a" => [1, 2, 3, 4, 5, 6],
            "c" => [10, 20, 30, 40, 50, 60]
        }?;

        assert!(result.equals(&expected));
        Ok(())
    }

    #[test]
    fn test_anonymous_scan_streaming_with_slice() -> PolarsResult<()> {
        let df1 = df! {
            "a" => [1, 2, 3, 4, 5],
            "b" => ["a", "b", "c", "d", "e"]
        }?;

        let scan = Arc::new(TestSingleScan::new(df1));

        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .with_streaming(true)
            .collect()?;
        let result = result.head(Some(3));

        let expected = df! {
            "a" => [1, 2, 3],
            "b" => ["a", "b", "c"]
        }?;

        assert!(result.equals(&expected));
        Ok(())
    }

    #[test]
    fn test_anonymous_scan_streaming_empty() -> PolarsResult<()> {
        let schema = Arc::new(Schema::from_iter(vec![
            ("a".into(), DataType::Int32),
            ("b".into(), DataType::String),
        ]));

        let scan = Arc::new(TestBatchScan {
            batches: Mutex::new(vec![]),
            schema,
            current_batch: Mutex::new(0),
            allows_predicate: false,
            allows_projection: false,
        });

        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .with_streaming(true)
            .collect()?;

        assert_eq!(result.height(), 0);
        assert_eq!(result.width(), 2); // Should preserve schema
        Ok(())
    }

    #[test]
    fn test_anonymous_scan_streaming_with_filter() -> PolarsResult<()> {
        let df1 = df! {
            "a" => [1, 2, 3],
            "b" => [10, 20, 30]
        }?;
        let df2 = df! {
            "a" => [4, 5, 6],
            "b" => [40, 50, 60]
        }?;

        let scan = Arc::new(TestBatchScan::new(vec![df1, df2]));

        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .filter(col("a").gt(lit(3)))
            .with_streaming(true)
            .collect()?;

        let expected = df! {
            "a" => [4, 5, 6],
            "b" => [40, 50, 60]
        }?;

        assert!(result.equals(&expected));
        Ok(())
    }

    #[test]
    fn test_anonymous_scan_streaming_comparison_with_eager() -> PolarsResult<()> {
        let df1 = df! {
            "value" => [1, 2, 3, 4, 5],
            "group" => ["A", "B", "A", "B", "A"]
        }?;

        let scan_eager = Arc::new(TestSingleScan::new(df1.clone()));
        let scan_streaming = Arc::new(TestSingleScan::new(df1));

        // Test eager
        let eager_result = LazyFrame::anonymous_scan(scan_eager, Default::default())?
            .group_by([col("group")])
            .agg([col("value").sum().alias("sum_value")])
            .sort(["group"], SortMultipleOptions::default())
            .collect()?;

        // Test streaming
        let streaming_result = LazyFrame::anonymous_scan(scan_streaming, Default::default())?
            .group_by([col("group")])
            .agg([col("value").sum().alias("sum_value")])
            .sort(["group"], SortMultipleOptions::default())
            .with_streaming(true)
            .collect()?;

        assert!(eager_result.equals(&streaming_result));
        Ok(())
    }

    #[test]
    fn test_anonymous_scan_streaming_large_batches() -> PolarsResult<()> {
        // Create multiple larger batches to test streaming behavior
        let mut batches = vec![];
        for i in 0..5 {
            let start = i * 1000;
            let end = (i + 1) * 1000;
            let values: Vec<i32> = (start..end).collect();
            let batch = df! {
                "id" => values.clone(),
                "value" => values.iter().map(|x| x * 2).collect::<Vec<i32>>()
            }?;
            batches.push(batch);
        }

        let scan = Arc::new(TestBatchScan::new(batches));

        let result = LazyFrame::anonymous_scan(scan, Default::default())?
            .with_streaming(true)
            .collect()?;

        assert_eq!(result.height(), 5000); // Should have all rows
        let total_value: i64 = result.column("value")?.i64()?.sum().unwrap();
        let expected_total: i64 = (0..5000).map(|x| (x * 2) as i64).sum();
        assert_eq!(total_value, expected_total);
        Ok(())
    }
}