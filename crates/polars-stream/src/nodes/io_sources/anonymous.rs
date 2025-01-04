use std::sync::Arc;
use std::sync::atomic::Ordering;
use polars_error::PolarsResult;
use polars_expr::prelude::ExecutionState;
use polars_plan::plans::{AnonymousScanOptions, ScanSources};
use polars_plan::prelude::{AnonymousScan};
use crate::async_executor::{JoinHandle, TaskScope};
use crate::graph::PortState;
use crate::nodes::ComputeNode;
use crate::pipe::{RecvPort, SendPort};

pub struct AnonymousSourceNode{
    function: Arc<dyn AnonymousScan>,
    options: Arc<AnonymousScanOptions>,
}

impl AnonymousSourceNode{
    pub fn new(function: Arc<dyn AnonymousScan>, options: Arc<AnonymousScanOptions>) -> Self {
        AnonymousSourceNode{
            function,
            options,
        }
    }

}

impl ComputeNode for AnonymousSourceNode {
    fn name(&self) -> &str {
        "anonymous_source"
    }

    fn update_state(&mut self, recv: &mut [PortState], send: &mut [PortState]) -> PolarsResult<()> {
        assert!(recv.is_empty());
        assert!(send.len() == 1);

        if send[0] == PortState::Done {
            send[0] = PortState::Done;
        } else {
            send[0] = PortState::Ready;
        }
        Ok(())
    }

    fn spawn<'env, 's>(&'env mut self, scope: &'s TaskScope<'s, 'env>, recv_ports: &mut [Option<RecvPort<'_>>], send_ports: &mut [Option<SendPort<'_>>], state: &'s ExecutionState, join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>) {
        todo!()
    }
}