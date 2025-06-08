use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};

/// Represents the type of aggregation to perform on a column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationType {
    Count,
    Sum,
    Average,
    Min,
    Max,
}

/// Represents an aggregation to be performed on a specific column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Aggregation {
    /// The column to aggregate
    pub column: Vec<u8>,
    /// The type of aggregation to perform
    pub aggregation_type: AggregationType,
}

/// Result of an aggregation operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationResult {
    Count(u64),
    Sum(i64),
    SumFloat(f64),
    Average(f64),
    Min(Vec<u8>),
    Max(Vec<u8>),
    Error(String),
}

impl AggregationResult {
    /// Convert the aggregation result to a string representation
    pub fn to_string(&self) -> String {
        match self {
            AggregationResult::Count(count) => format!("{}", count),
            AggregationResult::Sum(sum) => format!("{}", sum),
            AggregationResult::SumFloat(sum) => format!("{}", sum),
            AggregationResult::Average(avg) => format!("{}", avg),
            AggregationResult::Min(min) => format!("{:?}", min),
            AggregationResult::Max(max) => format!("{:?}", max),
            AggregationResult::Error(err) => format!("Error: {}", err),
        }
    }
}

/// Represents a set of aggregations to be performed on query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationSet {
    pub aggregations: Vec<Aggregation>,
}

impl AggregationSet {
    pub fn new() -> Self {
        AggregationSet {
            aggregations: Vec::new(),
        }
    }

    pub fn add_aggregation(&mut self, column: Vec<u8>, aggregation_type: AggregationType) -> &mut Self {
        self.aggregations.push(Aggregation {
            column,
            aggregation_type,
        });
        self
    }

    pub fn apply(&self, values: &BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>)>>) -> BTreeMap<Vec<u8>, AggregationResult> {
        let mut results = BTreeMap::new();

        for aggregation in &self.aggregations {
            let result = match values.get(&aggregation.column) {
                Some(column_values) => {
                    match aggregation.aggregation_type {
                        AggregationType::Count => {
                            AggregationResult::Count(column_values.len() as u64)
                        },
                        AggregationType::Sum => {
                            let result = column_values.iter()
                                .try_fold((0i64, 0.0f64, false), |(sum_i64, sum_f64, is_float), (_, value)| {
                                    let value_str = std::str::from_utf8(value)
                                        .map_err(|_| "Invalid UTF-8 in value")?;

                                    if let Ok(num) = value_str.parse::<i64>() {
                                        Ok((sum_i64 + num, sum_f64, is_float))
                                    } else if let Ok(num) = value_str.parse::<f64>() {
                                        Ok((sum_i64, sum_f64 + num, true))
                                    } else {
                                        Err("Non-numeric value found")
                                    }
                                });

                            match result {
                                Ok((sum_i64, sum_f64, is_float)) => {
                                    if is_float {
                                        AggregationResult::SumFloat(sum_f64)
                                    } else {
                                        AggregationResult::Sum(sum_i64)
                                    }
                                },
                                Err(err) => {
                                    return BTreeMap::from([(
                                        aggregation.column.clone(),
                                        AggregationResult::Error(err.to_string())
                                    )]);
                                }
                            }
                        },
                        AggregationType::Average => {
                            if column_values.is_empty() {
                                AggregationResult::Error("No values to average".to_string())
                            } else {
                                let result: Result<(f64, f64, Vec<(&u64, f64)>), &'static str> = column_values.iter()
                                    .try_fold((0.0, 0.0, Vec::new()), |(sum, count, mut debug_values), (ts, value)| {
                                        let value_str = std::str::from_utf8(value)
                                            .map_err(|_| "Invalid UTF-8 in value")?;

                                        let num = value_str.parse::<f64>()
                                            .map_err(|_| "Non-numeric value found")?;

                                        debug_values.push((ts, num));

                                        Ok((sum + num, count + 1.0, debug_values))
                                    });

                                match result {
                                    Ok((sum, count, _)) => {
                                        AggregationResult::Average(sum / count)
                                    },
                                    Err(err) => {
                                        return BTreeMap::from([(
                                            aggregation.column.clone(),
                                            AggregationResult::Error(err.to_string())
                                        )]);
                                    }
                                }
                            }
                        },
                        AggregationType::Min => {
                            if column_values.is_empty() {
                                AggregationResult::Error("No values to find minimum".to_string())
                            } else {
                                let min_value = column_values.iter()
                                    .map(|(_, v)| v)
                                    .min()
                                    .cloned()
                                    .unwrap();
                                AggregationResult::Min(min_value)
                            }
                        },
                        AggregationType::Max => {
                            if column_values.is_empty() {
                                AggregationResult::Error("No values to find maximum".to_string())
                            } else {
                                let max_value = column_values.iter()
                                    .map(|(_, v)| v)
                                    .max()
                                    .cloned()
                                    .unwrap();
                                AggregationResult::Max(max_value)
                            }
                        },
                    }
                },
                None => AggregationResult::Error(format!("Column not found: {:?}", aggregation.column)),
            };

            results.insert(aggregation.column.clone(), result);
        }

        results
    }
}

impl Default for AggregationSet {
    fn default() -> Self {
        Self::new()
    }
}
