import polars as pl
import unittest


def load_gold_data(path: str = "data/gold_data.parquet") -> pl.DataFrame:
    """
    Load the gold_data parquet file and return it as a Polars DataFrame.
    """
    return pl.read_parquet(path)


class TestGoldData(unittest.TestCase):
    """
    This class contains unittest to check the structure and values in the gold dataset. The class is organized as follows:
    1. setUp: Calls the load_gold_data function to read in the gold_data parquet.
    2. test_column_names_and_order: Checks the names and order of the columns. The test will pass if the columns have the correct names and are arranged in the expected order.
    3. test_datatypes: Checks the data types of each column. The test will pass of every column has the correct data type.
    4. test_missing_values: Checks for missing values in the gold dataset. The test will pass if no missing values are present.
    5. test_negative_deal_sizes: Checks for negative deal sizes. The test will pass if all deal sizes are nonnegative.
    """

    def setUp(self):
        self.gold_data = load_gold_data()

        self.expected_schema = {
            "dealid": pl.Utf8,
            "acquirer_ticker": pl.Utf8,
            "primaryindustrysector": pl.Utf8,
            "dealsize": pl.Float64,
            "market_cap_pre": pl.Float64,
            "ev_pre": pl.Float64,
            "ebitda_pre": pl.Float64,
            "market_cap_post": pl.Float64,
            "ev_post": pl.Float64,
            "ebitda_post": pl.Float64,
            "delta_ev_pct": pl.Float64,
            "delta_mkt_cap_pct": pl.Float64,
            "delta_ebitda_pct": pl.Float64,
            "deal_size_ratio": pl.Float64,
        }

        self.actual_schema = self.gold_data.schema

    def test_column_names_and_order(self):
        self.assertEqual(
            list(self.actual_schema.keys()),
            list(self.expected_schema.keys()),
            "Column names or order do not match the expected schema.",
        )

    def test_datatypes(self):
        for col, expected_type in self.expected_schema.items():
            self.assertEqual(
                self.actual_schema[col],
                expected_type,
                f"Column '{col}' has type {self.actual_schema[col]}, expected {expected_type}.",
            )

    def test_missing_values(self):
        total_missing = self.gold_data.null_count().to_series().sum()
        self.assertEqual(
            total_missing,
            0,
            f"The gold dataset contains {total_missing} missing values.",
        )

    def test_negative_deal_sizes(self):
        negative_deal_sizes = (self.gold_data["dealsize"] < 0).any()
        self.assertFalse(
            negative_deal_sizes,
            f"The gold dataset contains {negative_deal_sizes} negative deal sizes.",
        )


if __name__ == "__main__":
    unittest.main()
