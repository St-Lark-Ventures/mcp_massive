"""Tests for the filters module."""

import json
import pytest

from mcp_polygon.filters import (
    FilterOptions,
    parse_filter_params,
    apply_filters,
    FIELD_PRESETS,
    _apply_aggregation,
)


class TestFilterOptions:
    """Tests for FilterOptions dataclass."""

    def test_default_options(self):
        """Test that default options are set correctly."""
        options = FilterOptions()
        assert options.fields is None
        assert options.exclude_fields is None
        assert options.format == "csv"
        assert options.aggregate is None
        assert options.conditions is None

    def test_custom_options(self):
        """Test creating options with custom values."""
        options = FilterOptions(
            fields=["ticker", "close"], format="json", aggregate="last"
        )
        assert options.fields == ["ticker", "close"]
        assert options.format == "json"
        assert options.aggregate == "last"


class TestParseFilterParams:
    """Tests for parse_filter_params function."""

    def test_parse_no_params(self):
        """Test parsing with no parameters (defaults)."""
        options = parse_filter_params()
        assert options.fields is None
        assert options.format == "csv"
        assert options.aggregate is None

    def test_parse_comma_separated_fields(self):
        """Test parsing comma-separated fields."""
        options = parse_filter_params(fields="ticker,close,volume")
        assert options.fields == ["ticker", "close", "volume"]

    def test_parse_fields_with_spaces(self):
        """Test parsing fields with extra spaces."""
        options = parse_filter_params(fields="ticker, close , volume")
        assert options.fields == ["ticker", "close", "volume"]

    def test_parse_preset_fields(self):
        """Test parsing preset field names."""
        options = parse_filter_params(fields="preset:price")
        assert options.fields == FIELD_PRESETS["price"]

    def test_parse_preset_ohlc(self):
        """Test parsing OHLC preset."""
        options = parse_filter_params(fields="preset:ohlc")
        assert options.fields == ["ticker", "open", "high", "low", "close", "timestamp"]

    def test_parse_invalid_preset(self):
        """Test that invalid preset raises error."""
        with pytest.raises(ValueError, match="Unknown preset"):
            parse_filter_params(fields="preset:invalid_preset")

    def test_parse_output_format_csv(self):
        """Test parsing CSV output format."""
        options = parse_filter_params(output_format="csv")
        assert options.format == "csv"

    def test_parse_output_format_json(self):
        """Test parsing JSON output format."""
        options = parse_filter_params(output_format="json")
        assert options.format == "json"

    def test_parse_output_format_compact(self):
        """Test parsing compact output format."""
        options = parse_filter_params(output_format="compact")
        assert options.format == "compact"

    def test_parse_invalid_output_format(self):
        """Test that invalid output format raises error."""
        with pytest.raises(ValueError, match="Invalid output_format"):
            parse_filter_params(output_format="xml")

    def test_parse_aggregate_first(self):
        """Test parsing 'first' aggregation."""
        options = parse_filter_params(aggregate="first")
        assert options.aggregate == "first"

    def test_parse_aggregate_last(self):
        """Test parsing 'last' aggregation."""
        options = parse_filter_params(aggregate="last")
        assert options.aggregate == "last"

    def test_parse_invalid_aggregate(self):
        """Test that invalid aggregation raises error."""
        with pytest.raises(ValueError, match="Invalid aggregate"):
            parse_filter_params(aggregate="average")

    def test_parse_all_params(self):
        """Test parsing all parameters together."""
        options = parse_filter_params(
            fields="ticker,close", output_format="json", aggregate="last"
        )
        assert options.fields == ["ticker", "close"]
        assert options.format == "json"
        assert options.aggregate == "last"


class TestApplyAggregation:
    """Tests for _apply_aggregation function."""

    def test_aggregate_first_with_results_key(self):
        """Test 'first' aggregation with results key."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100},
                {"ticker": "AAPL", "close": 101},
                {"ticker": "AAPL", "close": 102},
            ]
        }
        result = _apply_aggregation(data, "first")
        assert result == {"results": [{"ticker": "AAPL", "close": 100}]}

    def test_aggregate_last_with_results_key(self):
        """Test 'last' aggregation with results key."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100},
                {"ticker": "AAPL", "close": 101},
                {"ticker": "AAPL", "close": 102},
            ]
        }
        result = _apply_aggregation(data, "last")
        assert result == {"results": [{"ticker": "AAPL", "close": 102}]}

    def test_aggregate_first_with_list(self):
        """Test 'first' aggregation with plain list."""
        data = [
            {"ticker": "AAPL", "close": 100},
            {"ticker": "AAPL", "close": 101},
        ]
        result = _apply_aggregation(data, "first")
        assert result == [{"ticker": "AAPL", "close": 100}]

    def test_aggregate_last_with_list(self):
        """Test 'last' aggregation with plain list."""
        data = [
            {"ticker": "AAPL", "close": 100},
            {"ticker": "AAPL", "close": 101},
        ]
        result = _apply_aggregation(data, "last")
        assert result == [{"ticker": "AAPL", "close": 101}]

    def test_aggregate_single_record(self):
        """Test aggregation with single record (no change)."""
        data = {"ticker": "AAPL", "close": 100}
        result = _apply_aggregation(data, "first")
        assert result == data

    def test_aggregate_empty_results(self):
        """Test aggregation with empty results."""
        data = {"results": []}
        result = _apply_aggregation(data, "first")
        assert result == data


class TestApplyFilters:
    """Tests for apply_filters function."""

    def test_apply_filters_csv_default(self):
        """Test applying filters with CSV output (default)."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100, "volume": 1000},
                {"ticker": "MSFT", "close": 200, "volume": 2000},
            ]
        }
        options = FilterOptions(format="csv")
        result = apply_filters(data, options)

        # Should be valid CSV
        assert "ticker,close,volume" in result
        assert "AAPL,100,1000" in result
        assert "MSFT,200,2000" in result

    def test_apply_filters_csv_with_field_selection(self):
        """Test applying filters with field selection."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100, "volume": 1000},
                {"ticker": "MSFT", "close": 200, "volume": 2000},
            ]
        }
        options = FilterOptions(fields=["ticker", "close"], format="csv")
        result = apply_filters(data, options)

        # Should only include selected fields
        assert "ticker,close" in result
        assert "volume" not in result
        assert "AAPL,100" in result
        assert "MSFT,200" in result

    def test_apply_filters_json_output(self):
        """Test applying filters with JSON output."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100},
            ]
        }
        options = FilterOptions(format="json")
        result = apply_filters(data, options)

        # Should be valid JSON
        parsed = json.loads(result)
        assert isinstance(parsed, list)
        assert len(parsed) == 1
        assert parsed[0]["ticker"] == "AAPL"
        assert parsed[0]["close"] == 100

    def test_apply_filters_compact_output(self):
        """Test applying filters with compact output."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100, "volume": 1000},
            ]
        }
        options = FilterOptions(format="compact")
        result = apply_filters(data, options)

        # Should be compact JSON (single record)
        parsed = json.loads(result)
        assert parsed["ticker"] == "AAPL"
        assert parsed["close"] == 100
        assert parsed["volume"] == 1000

    def test_apply_filters_compact_with_fields(self):
        """Test compact output with field selection."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100, "volume": 1000},
            ]
        }
        options = FilterOptions(fields=["close"], format="compact")
        result = apply_filters(data, options)

        # Should only include selected field
        parsed = json.loads(result)
        assert parsed == {"close": 100}

    def test_apply_filters_with_aggregation_first(self):
        """Test applying filters with 'first' aggregation."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100},
                {"ticker": "AAPL", "close": 101},
                {"ticker": "AAPL", "close": 102},
            ]
        }
        options = FilterOptions(aggregate="first", format="compact")
        result = apply_filters(data, options)

        parsed = json.loads(result)
        assert parsed["close"] == 100

    def test_apply_filters_with_aggregation_last(self):
        """Test applying filters with 'last' aggregation."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100},
                {"ticker": "AAPL", "close": 101},
                {"ticker": "AAPL", "close": 102},
            ]
        }
        options = FilterOptions(aggregate="last", format="compact")
        result = apply_filters(data, options)

        parsed = json.loads(result)
        assert parsed["close"] == 102

    def test_apply_filters_json_string_input(self):
        """Test applying filters with JSON string input."""
        data_str = json.dumps(
            {
                "results": [
                    {"ticker": "AAPL", "close": 100},
                ]
            }
        )
        options = FilterOptions(format="compact")
        result = apply_filters(data_str, options)

        parsed = json.loads(result)
        assert parsed["ticker"] == "AAPL"

    def test_apply_filters_nested_data(self):
        """Test applying filters with nested data (flattened)."""
        data = {
            "results": [
                {
                    "ticker": "AAPL",
                    "day": {
                        "close": 100,
                        "volume": 1000,
                    },
                }
            ]
        }
        options = FilterOptions(fields=["ticker", "day_close"], format="json")
        result = apply_filters(data, options)

        parsed = json.loads(result)
        assert parsed[0]["ticker"] == "AAPL"
        assert parsed[0]["day_close"] == 100
        assert "day_volume" not in parsed[0]

    def test_apply_filters_all_options(self):
        """Test applying all filter options together."""
        data = {
            "results": [
                {"ticker": "AAPL", "close": 100, "volume": 1000},
                {"ticker": "AAPL", "close": 101, "volume": 1100},
                {"ticker": "AAPL", "close": 102, "volume": 1200},
            ]
        }
        options = FilterOptions(fields=["close"], format="compact", aggregate="last")
        result = apply_filters(data, options)

        parsed = json.loads(result)
        assert parsed == {"close": 102}


class TestFieldPresets:
    """Tests for FIELD_PRESETS."""

    def test_price_preset_exists(self):
        """Test that 'price' preset exists."""
        assert "price" in FIELD_PRESETS
        assert "close" in FIELD_PRESETS["price"]

    def test_ohlc_preset_exists(self):
        """Test that 'ohlc' preset exists."""
        assert "ohlc" in FIELD_PRESETS
        assert "open" in FIELD_PRESETS["ohlc"]
        assert "high" in FIELD_PRESETS["ohlc"]
        assert "low" in FIELD_PRESETS["ohlc"]
        assert "close" in FIELD_PRESETS["ohlc"]

    def test_all_presets_are_lists(self):
        """Test that all presets are lists of strings."""
        for preset_name, fields in FIELD_PRESETS.items():
            assert isinstance(fields, list), f"{preset_name} should be a list"
            for field in fields:
                assert isinstance(field, str), (
                    f"Field in {preset_name} should be string"
                )
