from pathlib import Path

import polars as pl

# Download the postgres sql file from https://www.lipad.ca/data/ and create a table

uri = "postgresql://localhost:5432/cshansard"
# The downstream workflow assumes the ds is sorted!
query = """SELECT * from dilipadsite_basehansard ORDER BY basepk"""
OUTPUT_FOLDER_PATH = Path("/ca_hansard")
OUTPUT_FOLDER_PATH.mkdir(parents=True, exist_ok=True)

row_shifted = lambda column: (~pl.col(column).eq(pl.col(column).shift()))

(
    pl.read_database_uri(query=query, uri=uri)
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
        OUTPUT_FOLDER_PATH,
        use_pyarrow=True,
        pyarrow_options={"partition_cols": ["year"]},
    )
)
