use serde::{Deserialize, Serialize};
use regex::Regex as RegexPattern;

/// Filter represents a predicate that can be applied to cell values
/// to determine if they should be included in query results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Filter {
    Equal(Vec<u8>),
    NotEqual(Vec<u8>),
    GreaterThan(Vec<u8>),
    GreaterThanOrEqual(Vec<u8>),
    LessThan(Vec<u8>),
    LessThanOrEqual(Vec<u8>),
    Contains(Vec<u8>),
    StartsWith(Vec<u8>),
    EndsWith(Vec<u8>),
    /// Match values that match the given regex pattern
    /// The value must be valid UTF-8 and the pattern must be a valid regex
    /// Returns false if the value is not valid UTF-8 or the pattern is not a valid regex
    Regex(String),
    /// Combine multiple filters with AND logic (all must match)
    And(Vec<Filter>),
    /// Combine multiple filters with OR logic (any must match)
    Or(Vec<Filter>),
    /// Negate the result of the contained filter
    Not(Box<Filter>),
}

impl Filter {
    /// Apply the filter to a value and return true if it matches
    pub fn matches(&self, value: &[u8]) -> bool {
        match self {
            Filter::Equal(target) => value == target.as_slice(),
            Filter::NotEqual(target) => value != target.as_slice(),
            Filter::GreaterThan(target) => value > target.as_slice(),
            Filter::GreaterThanOrEqual(target) => value >= target.as_slice(),
            Filter::LessThan(target) => value < target.as_slice(),
            Filter::LessThanOrEqual(target) => value <= target.as_slice(),
            Filter::Contains(target) => contains_subsequence(value, target),
            Filter::StartsWith(target) => value.starts_with(target),
            Filter::EndsWith(target) => value.ends_with(target),
            Filter::Regex(pattern) => {
                if let Ok(str_value) = std::str::from_utf8(value) {
                    if let Ok(regex) = RegexPattern::new(pattern) {
                        regex.is_match(str_value)
                    } else {
                        false
                    }
                } else {
                    false
                }
            },
            Filter::And(filters) => filters.iter().all(|f| f.matches(value)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(value)),
            Filter::Not(filter) => !filter.matches(value),
        }
    }
}

fn contains_subsequence(value: &[u8], subsequence: &[u8]) -> bool {
    if subsequence.is_empty() {
        return true;
    }
    if subsequence.len() > value.len() {
        return false;
    }

    for i in 0..=(value.len() - subsequence.len()) {
        if value[i..(i + subsequence.len())] == subsequence[..] {
            return true;
        }
    }
    false
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFilter {
    pub column: Vec<u8>,
    pub filter: Filter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterSet {
    pub column_filters: Vec<ColumnFilter>,
    pub timestamp_range: Option<(Option<u64>, Option<u64>)>,
    pub max_versions: Option<usize>,
}

impl FilterSet {
    pub fn new() -> Self {
        FilterSet {
            column_filters: Vec::new(),
            timestamp_range: None,
            max_versions: None,
        }
    }

    pub fn add_column_filter(&mut self, column: Vec<u8>, filter: Filter) -> &mut Self {
        self.column_filters.push(ColumnFilter { column, filter });
        self
    }

    pub fn with_timestamp_range(&mut self, min: Option<u64>, max: Option<u64>) -> &mut Self {
        self.timestamp_range = Some((min, max));
        self
    }

    pub fn with_max_versions(&mut self, max_versions: usize) -> &mut Self {
        self.max_versions = Some(max_versions);
        self
    }

    pub fn timestamp_matches(&self, timestamp: u64) -> bool {
        if let Some((min, max)) = self.timestamp_range {
            let min_match = min.map_or(true, |min_ts| timestamp >= min_ts);
            let max_match = max.map_or(true, |max_ts| timestamp <= max_ts);
            min_match && max_match
        } else {
            true
        }
    }
}

impl Default for FilterSet {
    fn default() -> Self {
        Self::new()
    }
}
