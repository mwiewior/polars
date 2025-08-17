use std::sync::Arc;
use polars_core::prelude::*;
use polars_plan::prelude::{AnonymousScan, AnonymousScanOptions};
use crate::nodes::compute_node_prelude::*;
use crate::morsel::{Morsel, MorselSeq, SourceToken};
use polars_plan::plans::AnonymousScanArgs;

pub struct AnonymousSourceNode {
    function: Arc<dyn AnonymousScan>,
    _options: Arc<AnonymousScanOptions>,
}

impl AnonymousSourceNode {
    pub fn new(function: Arc<dyn AnonymousScan>, options: Arc<AnonymousScanOptions>) -> Self {
        Self { function, _options: options }
    }
}

impl ComputeNode for AnonymousSourceNode {
    fn name(&self) -> &str {
        "anonymous_source"
    }

    fn update_state(
        &mut self,
        _recv: &mut [PortState],
        send: &mut [PortState],
    ) -> PolarsResult<()> {
        assert!(_recv.is_empty());
        assert!(send.len() == 1);
        if send[0] == PortState::Done {
            return Ok(());
        }
        send[0] = PortState::Ready;
        Ok(())
    }

    fn spawn<'env, 's>(
        &'env mut self,
        scope: &'s TaskScope<'s, 'env>,
        _recv_ports: &mut [Option<RecvPort<'_>>],
        send_ports: &mut [Option<SendPort<'_>>],
        _state: &'s ExecutionState,
        join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>,
    ) {
        assert!(_recv_ports.is_empty() && send_ports.len() == 1);
        let mut send = send_ports[0].take().unwrap().serial();
        let function = self.function.clone();

        join_handles.push(scope.spawn_task(TaskPriority::Low, async move {
            let schema = function.schema(None)?;
            let mut morsel_seq = MorselSeq::new(0);
            let source_token = SourceToken::new();

            loop {
                let args = AnonymousScanArgs {
                    n_rows: None,
                    with_columns: None,
                    schema: schema.clone(),
                    output_schema: Some(schema.clone()),
                    predicate: None,
                    py: None,
                };
                match function.next_batch(args) {
                    Ok(Some(df)) => {
                        let morsel = Morsel::new(df, morsel_seq, source_token.clone());
                        if send.send(morsel).await.is_err() {
                            break;
                        }
                        morsel_seq = morsel_seq.successor();
                    }
                    Ok(None) => break,
                    Err(e) => return Err(e),
                }
            }

            Ok(())
        }));
    }
}
