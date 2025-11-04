# Output Filtering Guide

## Overview

The Polygon MCP server now supports server-side output filtering to dramatically reduce context token usage. You can select specific fields, choose output formats, and aggregate results.

## Quick Examples

### Example 1: Get Just the Closing Price

**Before (500 tokens):**
```python
result = await get_aggs("AAPL", 1, "day", "2024-01-01", "2024-01-02")
# Returns full CSV with all fields: ticker, open, high, low, close, volume, vwap, etc.
```

**After (30 tokens - 94% reduction!):**
```python
result = await get_aggs(
    "AAPL", 1, "day", "2024-01-01", "2024-01-02",
    fields="close",
    output_format="compact",
    aggregate="last"
)
# Returns: {"close": 185.92}
```

### Example 2: Get OHLC Data

**Before (2000 tokens):**
```python
result = await get_aggs("MSFT", 1, "day", "2024-01-01", "2024-01-08")
# Returns CSV with 15+ columns
```

**After (800 tokens - 60% reduction):**
```python
result = await get_aggs(
    "MSFT", 1, "day", "2024-01-01", "2024-01-08",
    fields="preset:ohlc",
    output_format="csv"
)
# Returns CSV with only: ticker, open, high, low, close, timestamp
```

### Example 3: Get Latest Trade Price

**Before:**
```python
result = await get_last_trade("AAPL")
# Returns all trade fields
```

**After:**
```python
result = await get_last_trade(
    "AAPL",
    fields="price",
    output_format="compact"
)
# Returns: {"price": 185.92}
```

## Filtering Parameters

### `fields` - Field Selection

Select specific fields to include in the response.

**Comma-separated field names:**
```python
fields="ticker,close,volume"
```

**Preset field groups:**
```python
fields="preset:price"     # ticker, close, timestamp
fields="preset:ohlc"      # ticker, open, high, low, close, timestamp
fields="preset:ohlcv"     # OHLC + volume
fields="preset:summary"   # ticker, close, volume, change_percent
fields="preset:trade"     # price, size, timestamp
fields="preset:quote"     # bid, ask, bid_size, ask_size, timestamp
```

### `output_format` - Output Format

Choose how data is formatted.

**CSV (default):**
```python
output_format="csv"
# Returns: ticker,close,volume\nAAPL,185.92,52165200
```

**JSON (formatted):**
```python
output_format="json"
# Returns: [
#   {
#     "ticker": "AAPL",
#     "close": 185.92,
#     "volume": 52165200
#   }
# ]
```

**Compact (minimal JSON):**
```python
output_format="compact"
# Returns: {"ticker":"AAPL","close":185.92,"volume":52165200}
```

### `aggregate` - Row Aggregation

Get a single record instead of all records.

**First record:**
```python
aggregate="first"
```

**Last record:**
```python
aggregate="last"
```

## Available Presets

| Preset | Fields |
|--------|--------|
| `preset:price` | ticker, close, timestamp |
| `preset:last_price` | close |
| `preset:ohlc` | ticker, open, high, low, close, timestamp |
| `preset:ohlcv` | ticker, open, high, low, close, volume, timestamp |
| `preset:summary` | ticker, close, volume, change_percent |
| `preset:minimal` | ticker, close |
| `preset:volume` | ticker, volume, timestamp |
| `preset:details` | ticker, name, market, locale, primary_exchange |
| `preset:info` | ticker, name, description, homepage_url |
| `preset:news_headlines` | title, published_utc, author |
| `preset:news_summary` | title, description, published_utc, article_url |
| `preset:trade` | price, size, timestamp |
| `preset:quote` | bid, ask, bid_size, ask_size, timestamp |

## Token Savings

| Use Case | Before | After | Savings |
|----------|--------|-------|---------|
| Single price query | ~500 tokens | ~30 tokens | **94%** |
| OHLC data (10 rows) | ~2000 tokens | ~800 tokens | **60%** |
| Ticker list (100 rows, 2 fields) | ~8000 tokens | ~1000 tokens | **87%** |
| News headlines (10 articles) | ~5000 tokens | ~600 tokens | **88%** |

## Tools with Filtering Support

Currently supported tools:
- `get_aggs` - Full filtering support (fields, output_format, aggregate)
- `get_last_trade` - Field and format filtering (fields, output_format)

**Coming soon:** All 50+ tools will support filtering.

## Backward Compatibility

All filtering parameters are **optional**. If you don't specify them, tools work exactly as before:

```python
# This still works - no changes needed to existing code
result = await get_aggs("AAPL", 1, "day", "2024-01-01", "2024-01-10")
# Returns: Full CSV with all fields (same as before)
```

## Best Practices

### 1. Use Compact Format for Single Values

When you only need one value from one record:
```python
# ✓ Good
result = await get_aggs(
    "AAPL", 1, "day", "2024-01-01", "2024-01-02",
    fields="close",
    output_format="compact",
    aggregate="last"
)
# Returns: {"close": 185.92}

# ✗ Avoid
result = await get_aggs("AAPL", 1, "day", "2024-01-01", "2024-01-02")
# Returns full CSV, then you parse it for one value
```

### 2. Use Presets for Common Patterns

Presets are easier to remember and type-safe:
```python
# ✓ Good
fields="preset:ohlc"

# ✗ Avoid
fields="ticker,open,high,low,close,timestamp"  # Easy to make typos
```

### 3. Use CSV for Multiple Records

CSV is more compact for tabular data:
```python
# ✓ Good for 10+ records
output_format="csv"

# ✗ JSON is verbose for many records
output_format="json"
```

### 4. Use Aggregation When Possible

If you only need the latest value:
```python
# ✓ Good
aggregate="last"  # Returns 1 record

# ✗ Avoid
# Get all records, then manually select the last one
```

## Advanced Usage

### Combining All Options

```python
result = await get_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-01-10",
    # Filtering
    fields="preset:ohlc",
    output_format="json",
    aggregate="last"
)
# Returns JSON with only OHLC fields from the last record
```

### Field Names for Nested Data

Nested fields are flattened with underscores:

Original structure:
```json
{
  "ticker": "AAPL",
  "day": {
    "close": 185.92,
    "volume": 52165200
  }
}
```

Flattened field names:
- `ticker`
- `day_close`
- `day_volume`

Usage:
```python
fields="ticker,day_close,day_volume"
```

## Error Handling

Invalid presets raise clear errors:
```python
fields="preset:invalid"
# Error: Unknown preset: invalid_preset. Available presets: price, ohlc, ohlcv, ...
```

Invalid formats raise clear errors:
```python
output_format="xml"
# Error: Invalid output_format: xml. Must be 'csv', 'json', or 'compact'
```

## Migration Guide

### Phase 1: Try It Out (No Changes Needed)
Your existing code works unchanged. Try filtering in new queries.

### Phase 2: Optimize Hot Paths
Identify queries that run frequently or return large responses. Add filtering:
```python
# Before
result = await get_aggs("AAPL", 1, "day", "2024-01-01", "2024-01-10")

# After
result = await get_aggs(
    "AAPL", 1, "day", "2024-01-01", "2024-01-10",
    fields="preset:price",
    output_format="compact",
    aggregate="last"
)
```

### Phase 3: Monitor Token Usage
Track token savings and adjust filtering as needed.

## FAQ

**Q: Does filtering slow down responses?**
A: No! Filtering reduces data processing and should slightly improve performance.

**Q: Can I exclude fields instead of including them?**
A: Not in the current version, but `exclude_fields` is planned for a future release.

**Q: What happens if I request a field that doesn't exist?**
A: The field is ignored. Only existing fields are returned.

**Q: Are filters cached?**
A: No, filtering happens on every request.

**Q: Can I use filtering with all tools?**
A: Currently only `get_aggs` and `get_last_trade` support filtering. All tools will be updated soon.

## Next Steps

1. Try filtering in your next query
2. Measure token savings
3. Gradually adopt filtering for frequently-used queries
4. Share feedback to help us improve!

## Support

For issues or questions:
- GitHub Issues: https://github.com/mobatmedia/mcp_polygon/issues
- See refactoring plan: `REFACTOR_PLAN.md`
