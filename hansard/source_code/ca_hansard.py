import argparse
from pathlib import Path

import polars as pl

# Download the postgres sql file from https://www.lipad.ca/data/ and create a postgres table

# The downstream workflow assumes the ds is sorted!
query = """SELECT * from dilipadsite_basehansard ORDER BY basepk"""
row_shifted = lambda column: (~pl.col(column).eq(pl.col(column).shift()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Consolidate Canadian Hansard into Dolma format."
    )
    parser.add_argument(
        "--output_folder",
        default="data/hansard/ca",
        help="Output format for parquet files",
    )
    parser.add_argument(
        "--uri",
        default="postgresql://localhost:5432/cshansard",
        help="URI to the database.",
    )
    args = parser.parse_args()
    URI = args.uri
    OUTPUT_FOLDER_PATH = Path(args.output_folder)
    OUTPUT_FOLDER_PATH.mkdir(parents=True, exist_ok=True)
    (
        pl.read_database_uri(query=query, uri=URI)
        .with_columns(pl.col(pl.Utf8).replace("", None))
        .with_columns(
            main_changed=row_shifted("maintopic"),
            minor_changed=row_shifted("subtopic"),
            speakerposition=pl.when(
                pl.col("speakerposition")
                .str.contains("(?i)interjection|intervention|stagedirection")
                .not_()
            )
            .then(None)
            .when(pl.col("speakerposition").str.contains("stagedirection"))
            .then(pl.lit("[Stage Direction]", dtype=pl.String))
            .otherwise(
                pl.concat_str(
                    pl.lit("[", dtype=pl.String),
                    pl.col("speakerposition").str.to_titlecase(),
                    pl.lit("]", dtype=pl.String),
                    ignore_nulls=False,
                )
            ),
        )
        .lazy()
        .group_by("speechdate")
        .agg(
            pl.concat_str(
                [
                    pl.when("main_changed")
                    .then(
                        pl.concat_str(
                            [
                                pl.lit("\n", dtype=pl.String),
                                pl.col("maintopic"),
                                pl.lit("\n", dtype=pl.String),
                            ],
                            ignore_nulls=False,
                        )
                    )
                    .otherwise(None),
                    pl.when("minor_changed").then(pl.col("subtopic")).otherwise(None),
                    pl.concat_str(
                        [pl.col("speakerposition"), pl.lit("\n", dtype=pl.String)],
                        ignore_nulls=False,
                    ),
                    pl.concat_str(
                        [
                            pl.when(pl.col("speakeroldname").str.contains("Speaker"))
                            .then(pl.col("speakeroldname"))
                            .when(pl.col("speakeroldname").is_null())
                            .then(pl.col("speakername"))
                            .when(pl.col("speakername").is_first_distinct())
                            .then(pl.col("speakeroldname"))
                            .otherwise(
                                pl.col("speakername"),
                            ),
                            pl.lit(": ", dtype=pl.String),
                        ],
                        ignore_nulls=False,
                    ),
                    pl.col("speechtext"),
                ],
                ignore_nulls=True,
            ).alias("text")
        )
        .with_columns(
            pl.col("text").list.join("\n").str.strip_chars(),
            year=pl.col("speechdate").dt.year(),
        )
        .collect()
        .write_parquet(
            f"{OUTPUT_FOLDER_PATH}.parquet",
            use_pyarrow=True,
            pyarrow_options={"partition_cols": ["year"]},
        )
    )
