//! APIs exposing `crate::parquet`'s statistics as arrow's statistics.
use std::collections::VecDeque;

use arrow::array::*;
use arrow::datatypes::{ArrowDataType, Field, IntervalUnit, PhysicalType};
use arrow::types::{f16, i256, NativeType};
use arrow::with_match_primitive_type_full;
use ethnum::I256;
use polars_error::{polars_bail, PolarsResult};

use crate::parquet::schema::types::{
    PhysicalType as ParquetPhysicalType, PrimitiveType as ParquetPrimitiveType,
};
use crate::parquet::statistics::{PrimitiveStatistics, Statistics as ParquetStatistics};
use crate::parquet::types::int96_to_i64_ns;
use crate::read::ColumnChunkMetadata;

mod binary;
mod binview;
mod boolean;
mod fixlen;
mod map;
mod null;
mod primitive;
mod struct_;
mod utf8;

use super::PrimitiveLogicalType;

/// Arrow-deserialized parquet Statistics of a file
#[derive(Debug, PartialEq)]
pub struct Statistics {
    /// number of nulls. This is a [`UInt64Array`] for non-nested types
    pub null_count: Box<dyn Array>,
    /// number of dictinct values. This is a [`UInt64Array`] for non-nested types
    pub distinct_count: Box<dyn Array>,
    /// Minimum
    pub min_value: Box<dyn Array>,
    /// Maximum
    pub max_value: Box<dyn Array>,
}

/// Arrow-deserialized parquet Statistics of a file
#[derive(Debug)]
struct MutableStatistics {
    /// number of nulls
    pub null_count: Box<dyn MutableArray>,
    /// number of dictinct values
    pub distinct_count: Box<dyn MutableArray>,
    /// Minimum
    pub min_value: Box<dyn MutableArray>,
    /// Maximum
    pub max_value: Box<dyn MutableArray>,
}

impl From<MutableStatistics> for Statistics {
    fn from(mut s: MutableStatistics) -> Self {
        let null_count = match s.null_count.dtype().to_physical_type() {
            PhysicalType::Struct => s
                .null_count
                .as_box()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone()
                .boxed(),
            _ => s
                .null_count
                .as_box()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone()
                .boxed(),
        };

        let distinct_count = match s.distinct_count.dtype().to_physical_type() {
            PhysicalType::Struct => s
                .distinct_count
                .as_box()
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone()
                .boxed(),
            _ => s
                .distinct_count
                .as_box()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone()
                .boxed(),
        };

        Self {
            null_count,
            distinct_count,
            min_value: s.min_value.as_box(),
            max_value: s.max_value.as_box(),
        }
    }
}

fn make_mutable(dtype: &ArrowDataType, capacity: usize) -> PolarsResult<Box<dyn MutableArray>> {
    Ok(match dtype.to_physical_type() {
        PhysicalType::Boolean => {
            Box::new(MutableBooleanArray::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        PhysicalType::Primitive(primitive) => with_match_primitive_type_full!(primitive, |$T| {
            Box::new(MutablePrimitiveArray::<$T>::with_capacity(capacity).to(dtype.clone()))
                as Box<dyn MutableArray>
        }),
        PhysicalType::Binary => {
            Box::new(MutableBinaryArray::<i32>::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        PhysicalType::LargeBinary => {
            Box::new(MutableBinaryArray::<i64>::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        PhysicalType::Utf8 => {
            Box::new(MutableUtf8Array::<i32>::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        PhysicalType::LargeUtf8 => {
            Box::new(MutableUtf8Array::<i64>::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        PhysicalType::FixedSizeBinary => {
            Box::new(MutableFixedSizeBinaryArray::try_new(dtype.clone(), vec![], None).unwrap())
                as _
        },
        PhysicalType::LargeList | PhysicalType::List | PhysicalType::FixedSizeList => {
            make_mutable(dtype.inner_dtype().unwrap(), capacity)?
        },
        PhysicalType::Dictionary(_) => {
            let ArrowDataType::Dictionary(_, dtype, _) = &dtype else {
                unreachable!();
            };

            make_mutable(dtype, capacity)?
        },
        PhysicalType::Struct => Box::new(struct_::DynMutableStructArray::try_with_capacity(
            dtype.clone(),
            capacity,
        )?),
        PhysicalType::Map => Box::new(map::DynMutableMapArray::try_with_capacity(
            dtype.clone(),
            capacity,
        )?),
        PhysicalType::Null => {
            Box::new(MutableNullArray::new(ArrowDataType::Null, 0)) as Box<dyn MutableArray>
        },
        PhysicalType::BinaryView => {
            Box::new(MutablePlBinary::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        PhysicalType::Utf8View => {
            Box::new(MutablePlString::with_capacity(capacity)) as Box<dyn MutableArray>
        },
        other => {
            polars_bail!(
                nyi = "deserializing parquet stats from {other:?} is still not implemented"
            )
        },
    })
}

fn create_dt(dtype: &ArrowDataType) -> ArrowDataType {
    match dtype.to_logical_type() {
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(
            fields
                .iter()
                .map(|f| Field::new(f.name.clone(), create_dt(&f.dtype), f.is_nullable))
                .collect(),
        ),
        ArrowDataType::Map(f, ordered) => ArrowDataType::Map(
            Box::new(Field::new(
                f.name.clone(),
                create_dt(&f.dtype),
                f.is_nullable,
            )),
            *ordered,
        ),
        ArrowDataType::LargeList(f) => ArrowDataType::LargeList(Box::new(Field::new(
            f.name.clone(),
            create_dt(&f.dtype),
            f.is_nullable,
        ))),
        // FixedSizeList piggy backs on list
        ArrowDataType::List(f) | ArrowDataType::FixedSizeList(f, _) => {
            ArrowDataType::List(Box::new(Field::new(
                f.name.clone(),
                create_dt(&f.dtype),
                f.is_nullable,
            )))
        },
        _ => ArrowDataType::UInt64,
    }
}

impl MutableStatistics {
    fn try_new(field: &Field) -> PolarsResult<Self> {
        let min_value = make_mutable(&field.dtype, 0)?;
        let max_value = make_mutable(&field.dtype, 0)?;

        let dt = create_dt(&field.dtype);
        Ok(Self {
            null_count: make_mutable(&dt, 0)?,
            distinct_count: make_mutable(&dt, 0)?,
            min_value,
            max_value,
        })
    }
}

fn push_others(
    from: Option<&ParquetStatistics>,
    distinct_count: &mut UInt64Vec,
    null_count: &mut UInt64Vec,
) {
    let from = if let Some(from) = from {
        from
    } else {
        distinct_count.push(None);
        null_count.push(None);
        return;
    };
    use ParquetStatistics as S;
    let (distinct, null_count1) = match from {
        S::Binary(s) => (s.distinct_count, s.null_count),
        S::Boolean(s) => (s.distinct_count, s.null_count),
        S::FixedLen(s) => (s.distinct_count, s.null_count),
        S::Int32(s) => (s.distinct_count, s.null_count),
        S::Int64(s) => (s.distinct_count, s.null_count),
        S::Int96(s) => (s.distinct_count, s.null_count),
        S::Float(s) => (s.distinct_count, s.null_count),
        S::Double(s) => (s.distinct_count, s.null_count),
    };

    distinct_count.push(distinct.map(|x| x as u64));
    null_count.push(null_count1.map(|x| x as u64));
}

fn push(
    stats: &mut VecDeque<(Option<ParquetStatistics>, ParquetPrimitiveType)>,
    min: &mut dyn MutableArray,
    max: &mut dyn MutableArray,
    distinct_count: &mut dyn MutableArray,
    null_count: &mut dyn MutableArray,
) -> PolarsResult<()> {
    let mut logical_type = min.dtype().to_logical_type();

    loop {
        if let List(field) | LargeList(field) | FixedSizeList(field, _) = logical_type {
            logical_type = field.dtype().to_logical_type();
            continue;
        }
        if let Dictionary(_, dt, _) = logical_type {
            logical_type = dt.to_logical_type();
        }

        break;
    }

    match logical_type {
        Struct(fields) => {
            if fields.is_empty() {
                return Ok(());
            }

            let min = min
                .as_mut_any()
                .downcast_mut::<struct_::DynMutableStructArray>()
                .unwrap();
            let max = max
                .as_mut_any()
                .downcast_mut::<struct_::DynMutableStructArray>()
                .unwrap();
            let distinct_count = distinct_count
                .as_mut_any()
                .downcast_mut::<struct_::DynMutableStructArray>()
                .unwrap();
            let null_count = null_count
                .as_mut_any()
                .downcast_mut::<struct_::DynMutableStructArray>()
                .unwrap();

            return min
                .inner_mut()
                .iter_mut()
                .zip(max.inner_mut())
                .zip(distinct_count.inner_mut())
                .zip(null_count.inner_mut())
                .try_for_each(|(((min, max), distinct_count), null_count)| {
                    push(
                        stats,
                        min.as_mut(),
                        max.as_mut(),
                        distinct_count.as_mut(),
                        null_count.as_mut(),
                    )
                });
        },
        Map(_, _) => {
            let min = min
                .as_mut_any()
                .downcast_mut::<map::DynMutableMapArray>()
                .unwrap();
            let max = max
                .as_mut_any()
                .downcast_mut::<map::DynMutableMapArray>()
                .unwrap();
            let distinct_count = distinct_count
                .as_mut_any()
                .downcast_mut::<map::DynMutableMapArray>()
                .unwrap();
            let null_count = null_count
                .as_mut_any()
                .downcast_mut::<map::DynMutableMapArray>()
                .unwrap();
            return push(
                stats,
                min.inner.as_mut(),
                max.inner.as_mut(),
                distinct_count.inner.as_mut(),
                null_count.inner.as_mut(),
            );
        },
        _ => {},
    }

    let (from, type_) = stats.pop_front().unwrap();
    let from = from.as_ref();

    let distinct_count = distinct_count
        .as_mut_any()
        .downcast_mut::<UInt64Vec>()
        .unwrap();
    let null_count = null_count.as_mut_any().downcast_mut::<UInt64Vec>().unwrap();

    push_others(from, distinct_count, null_count);

    let physical_type = &type_.physical_type;

    macro_rules! rmap {
        ($from:expr, $map:ident) => {{
            $from.map(ParquetStatistics::$map)
        }};
    }

    use ArrowDataType::*;
    use ParquetPhysicalType as PPT;
    match min.dtype().to_logical_type() {
        Boolean => boolean::push(rmap!(from, expect_as_boolean), min, max),
        Int8 => primitive::push(rmap!(from, expect_as_int32), min, max, |x: i32| Ok(x as i8)),
        Int16 => primitive::push(
            rmap!(from, expect_as_int32),
            min,
            max,
            |x: i32| Ok(x as i16),
        ),
        Date32 | Time32(_) => {
            primitive::push::<i32, i32, _>(rmap!(from, expect_as_int32), min, max, Ok)
        },
        Interval(IntervalUnit::YearMonth) => {
            fixlen::push_year_month(rmap!(from, expect_as_fixedlen), min, max)
        },
        Interval(IntervalUnit::DayTime) => {
            fixlen::push_days_ms(rmap!(from, expect_as_fixedlen), min, max)
        },
        UInt8 => primitive::push(rmap!(from, expect_as_int32), min, max, |x: i32| Ok(x as u8)),
        UInt16 => primitive::push(
            rmap!(from, expect_as_int32),
            min,
            max,
            |x: i32| Ok(x as u16),
        ),
        UInt32 => match physical_type {
            // some implementations of parquet write arrow's u32 into i64.
            PPT::Int64 => {
                primitive::push(
                    rmap!(from, expect_as_int64),
                    min,
                    max,
                    |x: i64| Ok(x as u32),
                )
            },
            PPT::Int32 => {
                primitive::push(
                    rmap!(from, expect_as_int32),
                    min,
                    max,
                    |x: i32| Ok(x as u32),
                )
            },
            other => polars_bail!(nyi = "Can't decode UInt32 type from parquet type {other:?}"),
        },
        Int32 => primitive::push::<i32, i32, _>(rmap!(from, expect_as_int32), min, max, Ok),
        Date64 => match physical_type {
            PPT::Int64 => {
                primitive::push::<i64, i64, _>(rmap!(from, expect_as_int64), min, max, Ok)
            },
            // some implementations of parquet write arrow's date64 into i32.
            PPT::Int32 => primitive::push(rmap!(from, expect_as_int32), min, max, |x: i32| {
                Ok(x as i64 * 86400000)
            }),
            other => polars_bail!(nyi = "Can't decode Date64 type from parquet type {other:?}"),
        },
        Int64 | Time64(_) | Duration(_) => {
            primitive::push::<i64, i64, _>(rmap!(from, expect_as_int64), min, max, Ok)
        },
        UInt64 => primitive::push(
            rmap!(from, expect_as_int64),
            min,
            max,
            |x: i64| Ok(x as u64),
        ),
        Timestamp(time_unit, _) => {
            let time_unit = *time_unit;
            if physical_type == &PPT::Int96 {
                let from = from.map(|from| {
                    let from = from.expect_as_int96();
                    PrimitiveStatistics::<i64> {
                        primitive_type: from.primitive_type.clone(),
                        null_count: from.null_count,
                        distinct_count: from.distinct_count,
                        min_value: from.min_value.map(int96_to_i64_ns),
                        max_value: from.max_value.map(int96_to_i64_ns),
                    }
                });
                primitive::push(from.as_ref(), min, max, |x: i64| {
                    Ok(primitive::timestamp(
                        type_.logical_type.as_ref(),
                        time_unit,
                        x,
                    ))
                })
            } else {
                primitive::push(rmap!(from, expect_as_int64), min, max, |x: i64| {
                    Ok(primitive::timestamp(
                        type_.logical_type.as_ref(),
                        time_unit,
                        x,
                    ))
                })
            }
        },
        Float32 => primitive::push::<f32, f32, _>(rmap!(from, expect_as_float), min, max, Ok),
        Float64 => primitive::push::<f64, f64, _>(rmap!(from, expect_as_double), min, max, Ok),
        Decimal(_, _) => match physical_type {
            PPT::Int32 => primitive::push(rmap!(from, expect_as_int32), min, max, |x: i32| {
                Ok(x as i128)
            }),
            PPT::Int64 => primitive::push(rmap!(from, expect_as_int64), min, max, |x: i64| {
                Ok(x as i128)
            }),
            PPT::FixedLenByteArray(n) if *n > 16 => polars_bail!(
                nyi = "Can't decode Decimal128 type from Fixed Size Byte Array of len {n:?}"
            ),
            PPT::FixedLenByteArray(n) => {
                fixlen::push_i128(rmap!(from, expect_as_fixedlen), *n, min, max)
            },
            _ => unreachable!(),
        },
        Decimal256(_, _) => match physical_type {
            PPT::Int32 => primitive::push(rmap!(from, expect_as_int32), min, max, |x: i32| {
                Ok(i256(I256::new(x.into())))
            }),
            PPT::Int64 => primitive::push(rmap!(from, expect_as_int64), min, max, |x: i64| {
                Ok(i256(I256::new(x.into())))
            }),
            PPT::FixedLenByteArray(n) if *n <= 16 => {
                fixlen::push_i256_with_i128(rmap!(from, expect_as_fixedlen), *n, min, max)
            },
            PPT::FixedLenByteArray(n) if *n > 32 => polars_bail!(
                nyi = "Can't decode Decimal256 type from Fixed Size Byte Array of len {n:?}"
            ),
            PPT::FixedLenByteArray(_) => {
                fixlen::push_i256(rmap!(from, expect_as_fixedlen), min, max)
            },
            _ => unreachable!(),
        },
        Binary => binary::push::<i32>(rmap!(from, expect_as_binary), min, max),
        LargeBinary => binary::push::<i64>(rmap!(from, expect_as_binary), min, max),
        Utf8 => utf8::push::<i32>(rmap!(from, expect_as_binary), min, max),
        LargeUtf8 => utf8::push::<i64>(rmap!(from, expect_as_binary), min, max),
        BinaryView => binview::push::<[u8]>(rmap!(from, expect_as_binary), min, max),
        Utf8View => binview::push::<str>(rmap!(from, expect_as_binary), min, max),
        FixedSizeBinary(_) => fixlen::push(rmap!(from, expect_as_fixedlen), min, max),

        Null => null::push(min, max),
        other => todo!("{:?}", other),
    }
}

pub fn cast_statistics(
    statistics: ParquetStatistics,
    primitive_type: &ParquetPrimitiveType,
    output_type: &ArrowDataType,
) -> ParquetStatistics {
    use {ArrowDataType as DT, PrimitiveLogicalType as PT};

    match (primitive_type.logical_type, output_type) {
        (Some(PT::Float16), DT::Float32) => {
            let statistics = statistics.expect_fixedlen();

            let primitive_type = primitive_type.clone();

            ParquetStatistics::Float(PrimitiveStatistics::<f32> {
                primitive_type,
                null_count: statistics.null_count,
                distinct_count: statistics.distinct_count,
                min_value: statistics
                    .min_value
                    .as_ref()
                    .map(|v| f16::from_le_bytes([v[0], v[1]]).to_f32()),
                max_value: statistics
                    .max_value
                    .as_ref()
                    .map(|v| f16::from_le_bytes([v[0], v[1]]).to_f32()),
            })
        },
        _ => statistics,
    }
}

/// Deserializes the statistics in the column chunks from a single `row_group`
/// into [`Statistics`] associated from `field`'s name.
///
/// # Errors
/// This function errors if the deserialization of the statistics fails (e.g. invalid utf8)
pub fn deserialize<'a>(
    field: &Field,
    field_md: impl ExactSizeIterator<Item = &'a ColumnChunkMetadata>,
) -> PolarsResult<Statistics> {
    let mut statistics = MutableStatistics::try_new(field)?;

    let mut stats = field_md
        .map(|column| {
            let primitive_type = &column.descriptor().descriptor.primitive_type;
            Ok((
                column
                    .statistics()
                    .transpose()?
                    .map(|stats| cast_statistics(stats, primitive_type, &field.dtype)),
                primitive_type.clone(),
            ))
        })
        .collect::<PolarsResult<VecDeque<(Option<ParquetStatistics>, ParquetPrimitiveType)>>>()?;
    push(
        &mut stats,
        statistics.min_value.as_mut(),
        statistics.max_value.as_mut(),
        statistics.distinct_count.as_mut(),
        statistics.null_count.as_mut(),
    )?;

    Ok(statistics.into())
}
