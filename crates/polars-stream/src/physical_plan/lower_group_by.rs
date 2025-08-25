use std::sync::Arc;

use parking_lot::Mutex;
use polars_core::prelude::{InitHashMaps, PlIndexMap};
use polars_core::schema::Schema;
use polars_error::{PolarsResult, polars_err};
use polars_expr::state::ExecutionState;
use polars_mem_engine::create_physical_plan;
use polars_plan::plans::expr_ir::{ExprIR, OutputName};
use polars_plan::plans::{AExpr, DataFrameUdf, IR, IRAggExpr};
use polars_plan::prelude::GroupbyOptions;
use polars_utils::arena::{Arena, Node};
use polars_utils::pl_str::PlSmallStr;
use polars_utils::unique_column_name;
use recursive::recursive;
use slotmap::SlotMap;

use super::{ExprCache, PhysNode, PhysNodeKey, PhysNodeKind, PhysStream};
use crate::physical_plan::lower_expr::{
    build_select_stream, compute_output_schema, is_elementwise_rec_cached,
    is_fake_elementwise_function, is_input_independent,
};
use crate::physical_plan::lower_ir::build_slice_stream;
use crate::utils::late_materialized_df::LateMaterializedDataFrame;

#[allow(clippy::too_many_arguments)]
fn build_group_by_fallback(
    input: PhysStream,
    keys: &[ExprIR],
    aggs: &[ExprIR],
    output_schema: Arc<Schema>,
    maintain_order: bool,
    options: Arc<GroupbyOptions>,
    apply: Option<Arc<dyn DataFrameUdf>>,
    expr_arena: &mut Arena<AExpr>,
    phys_sm: &mut SlotMap<PhysNodeKey, PhysNode>,
) -> PolarsResult<PhysStream> {
    let input_schema = phys_sm[input.node].output_schema.clone();
    let lmdf = Arc::new(LateMaterializedDataFrame::default());
    let mut lp_arena = Arena::default();
    let input_lp_node = lp_arena.add(lmdf.clone().as_ir_node(input_schema.clone()));
    let group_by_lp_node = lp_arena.add(IR::GroupBy {
        input: input_lp_node,
        keys: keys.to_vec(),
        aggs: aggs.to_vec(),
        schema: output_schema.clone(),
        maintain_order,
        options,
        apply,
    });
    let executor = Mutex::new(create_physical_plan(
        group_by_lp_node,
        &mut lp_arena,
        expr_arena,
        None,
    )?);

    let group_by_node = PhysNode {
        output_schema,
        kind: PhysNodeKind::InMemoryMap {
            input,
            map: Arc::new(move |df| {
                lmdf.set_materialized_dataframe(df);
                let mut state = ExecutionState::new();
                executor.lock().execute(&mut state)
            }),
        },
    };

    Ok(PhysStream::first(phys_sm.insert(group_by_node)))
}

/// Tries to lower an expression as a 'elementwise scalar agg expression'.
///
/// Such an expression is defined as the elementwise combination of scalar
/// aggregations of elementwise combinations of the input columns / scalar literals.
#[recursive]
fn try_lower_elementwise_scalar_agg_expr(
    expr: Node,
    outer_name: Option<PlSmallStr>,
    // expr_merger: &NaiveExprMerger,
    expr_cache: &mut ExprCache,
    expr_arena: &mut Arena<AExpr>,
    agg_exprs: &mut Vec<ExprIR>,
    uniq_input_exprs: &mut PlIndexMap<u32, PlSmallStr>,
) -> Option<Node> {
    // Temporarily disable due to missing NaiveExprMerger
    return None;
}

#[allow(clippy::too_many_arguments)]
fn try_build_streaming_group_by(
    input: PhysStream,
    keys: &[ExprIR],
    aggs: &[ExprIR],
    maintain_order: bool,
    options: Arc<GroupbyOptions>,
    apply: Option<Arc<dyn DataFrameUdf>>,
    expr_arena: &mut Arena<AExpr>,
    phys_sm: &mut SlotMap<PhysNodeKey, PhysNode>,
    expr_cache: &mut ExprCache,
) -> Option<PolarsResult<PhysStream>> {
    // Temporarily disable streaming group by due to missing NaiveExprMerger
    return None;
}

#[allow(clippy::too_many_arguments)]
pub fn build_group_by_stream(
    input: PhysStream,
    keys: &[ExprIR],
    aggs: &[ExprIR],
    output_schema: Arc<Schema>,
    maintain_order: bool,
    options: Arc<GroupbyOptions>,
    apply: Option<Arc<dyn DataFrameUdf>>,
    expr_arena: &mut Arena<AExpr>,
    phys_sm: &mut SlotMap<PhysNodeKey, PhysNode>,
    expr_cache: &mut ExprCache,
) -> PolarsResult<PhysStream> {
    let streaming = try_build_streaming_group_by(
        input,
        keys,
        aggs,
        maintain_order,
        options.clone(),
        apply.clone(),
        expr_arena,
        phys_sm,
        expr_cache,
    );
    if let Some(stream) = streaming {
        stream
    } else {
        build_group_by_fallback(
            input,
            keys,
            aggs,
            output_schema,
            maintain_order,
            options,
            apply,
            expr_arena,
            phys_sm,
        )
    }
}