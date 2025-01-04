use std::sync::Arc;

use polars_core::prelude::*;
use polars_plan::prelude::{AnonymousScan, AnonymousScanOptions};

use crate::graph::GraphNode;
use crate::morsel::{Morsel, MorselSeq};
use crate::nodes::ComputeNode;

pub struct AnonymousSourceNode {
    function: Arc<dyn AnonymousScan>,
    options: Arc<AnonymousScanOptions>,
}

impl AnonymousSourceNode {
    pub fn new(function: Arc<dyn AnonymousScan>, options: Arc<AnonymousScanOptions>) -> Self {
        Self { function, options }
    }
}

impl GraphNode for AnonymousSourceNode {
    fn name(&self) -> &'static str {
        "anonymous_source"
    }

    fn is_source(&self) -> bool {
        true
    }

    fn make_compute(
        &self,
        _morsel_seq: MorselSeq,
        _morsel_size: usize,
        _num_pipelines: usize,
    ) -> Arc<dyn ComputeNode> {
        Arc::new(AnonymousSourceComputeNode {
            function: self.function.clone(),
            options: self.options.clone(),
            finished: false,
        })
    }
}

struct AnonymousSourceComputeNode {
    function: Arc<dyn AnonymousScan>,
    options: Arc<AnonymousScanOptions>,
    finished: bool,
}

impl ComputeNode for AnonymousSourceComputeNode {
    fn name(&self) -> &'static str {
        "anonymous_source"
    }

    fn run(
        &mut self,
        _state: &mut crate::execute::SharedMorselState,
        _morsel_seq: MorselSeq,
    ) -> PolarsResult<Vec<Morsel>> {
        if self.finished {
            return Ok(vec![]);
        }
        self.finished = true;
        let df = self.function.scan(self.options.as_ref().clone())?;
        Ok(vec![Morsel::new(df)])
    }
}
