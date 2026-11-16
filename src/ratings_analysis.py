import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, explode, split


LOGGER = logging.getLogger("movielens")
HDFS_CLUSTER = "hdfs://master:9000"
OUTPUT_NAMES = (
    "summary",
    "top_users",
    "top_movies",
    "genre_stats",
    "rating_distribution",
)


def _read_boolean_env(name, default):
    value = os.getenv(name, default).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be 'true' or 'false', not {value!r}.")
    return value == "true"


def _spark_configuration():
    spark_conf = SparkConf(loadDefaults=True)
    loaded_settings = dict(spark_conf.getAll())
    submitted_master = loaded_settings.get("spark.master")
    requested_mode = os.getenv("MOVIELENS_EXECUTION_MODE", "").strip().lower()

    if requested_mode and requested_mode not in {"local", "cluster"}:
        raise ValueError(
            "MOVIELENS_EXECUTION_MODE must be 'local' or 'cluster'."
        )

    if requested_mode == "local":
        spark_conf.setMaster("local[*]")
        mode = "local"
    elif requested_mode == "cluster":
        spark_conf.setMaster("spark://master:7077")
        mode = "cluster"
    elif submitted_master:
        mode = "cluster" if submitted_master.startswith("spark://") else "local"
    else:
        spark_conf.setMaster("local[*]")
        mode = "local"

    master = dict(spark_conf.getAll()).get("spark.master")
    LOGGER.info("Execution mode: %s (Spark master: %s)", mode, master)
    return mode, spark_conf


def _output_mode(overwrite):
    return "overwrite" if overwrite else "errorifexists"


def _path_exists_in_hdfs(path):
    result = subprocess.run(
        ["hdfs", "dfs", "-test", "-e", path],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode not in {0, 1}:
        raise RuntimeError(
            f"Could not check HDFS output path {path}: {result.stderr.strip()}"
        )
    return result.returncode == 0


def _preflight_outputs(mode, output_names, overwrite, sync_outputs):
    if mode == "local":
        output_dir = Path(
            os.path.expanduser("~/MovieLensProject/data/processed")
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        existing = [name for name in output_names if (output_dir / name).exists()]
        if existing and not overwrite:
            raise FileExistsError(
                "Local output directories already exist: "
                f"{', '.join(existing)}. Set MOVIELENS_OVERWRITE=true "
                "to intentionally replace them."
            )
        return

    subprocess.run(
        ["hdfs", "dfs", "-ls", f"{HDFS_CLUSTER}/movielens"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    hdfs_output_root = f"{HDFS_CLUSTER}/movielens/processed"
    existing_hdfs = [
        name
        for name in output_names
        if _path_exists_in_hdfs(f"{hdfs_output_root}/{name}")
    ]
    if existing_hdfs and not overwrite:
        raise FileExistsError(
            "HDFS output directories already exist: "
            f"{', '.join(existing_hdfs)}. Set MOVIELENS_OVERWRITE=true "
            "to intentionally replace them."
        )

    if sync_outputs:
        local_output_dir = Path("/workspace/data/processed")
        local_output_dir.mkdir(parents=True, exist_ok=True)
        existing_local = [
            name
            for name in output_names
            if (local_output_dir / name).exists()
        ]
        if existing_local and not overwrite:
            raise FileExistsError(
                "Local dashboard output directories already exist: "
                f"{', '.join(existing_local)}. Set MOVIELENS_OVERWRITE=true "
                "to intentionally replace them."
            )


def _remove_path(path):
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.exists():
        shutil.rmtree(path)


def _sync_hdfs_outputs_to_local(output_names, overwrite):
    local_output_dir = Path("/workspace/data/processed")
    local_output_dir.mkdir(parents=True, exist_ok=True)
    stage_root = Path(
        tempfile.mkdtemp(
            prefix=".movielens-stage-",
            dir=local_output_dir,
        )
    )
    backup_root = Path(
        tempfile.mkdtemp(
            prefix=".movielens-backup-",
            dir=local_output_dir,
        )
    )
    moved_to_backup = []
    installed = []
    committed = False

    try:
        for name in output_names:
            source = f"{HDFS_CLUSTER}/movielens/processed/{name}"
            staged_output = stage_root / name
            LOGGER.info("Copying HDFS output %s to a staging directory.", source)
            subprocess.run(
                ["hdfs", "dfs", "-get", source, str(staged_output)],
                check=True,
            )
            if not list(staged_output.glob("part-*.csv")):
                raise RuntimeError(
                    f"HDFS output {source} has no part-*.csv file."
                )

        for name in output_names:
            destination = local_output_dir / name
            staged_output = stage_root / name
            backup = backup_root / name
            if destination.exists() or destination.is_symlink():
                if not overwrite:
                    raise FileExistsError(
                        f"Refusing to replace existing output {destination}."
                    )
                os.replace(destination, backup)
                moved_to_backup.append(name)
            os.replace(staged_output, destination)
            installed.append(name)
        committed = True
        LOGGER.info(
            "Copied all HDFS outputs to %s.", local_output_dir
        )
    except Exception:
        for name in reversed(installed):
            _remove_path(local_output_dir / name)
        for name in reversed(moved_to_backup):
            backup = backup_root / name
            if backup.exists() or backup.is_symlink():
                os.replace(backup, local_output_dir / name)
        raise
    finally:
        _remove_path(stage_root)
        if committed or not any(backup_root.iterdir()):
            _remove_path(backup_root)
        else:
            LOGGER.error(
                "Could not restore every previous output; backup data retained at %s.",
                backup_root,
            )


def _write_output(dataframe, output_path, mode):
    dataframe.coalesce(1).write \
        .mode(mode) \
        .option("header", True) \
        .csv(output_path)


def _run_analytics():
    mode, spark_conf = _spark_configuration()
    overwrite = _read_boolean_env("MOVIELENS_OVERWRITE", "false")
    sync_outputs = (
        _read_boolean_env("MOVIELENS_SYNC_OUTPUTS", "true")
        if mode == "cluster"
        else False
    )
    output_names = OUTPUT_NAMES
    _preflight_outputs(mode, output_names, overwrite, sync_outputs)

    hdfs_root = (
        HDFS_CLUSTER if mode == "cluster" else "hdfs://localhost:9000"
    )
    input_root = f"{hdfs_root}/movielens"
    if mode == "cluster":
        output_root = f"{HDFS_CLUSTER}/movielens/processed"
    else:
        output_root = os.path.expanduser(
            "~/MovieLensProject/data/processed"
        )
    write_mode = _output_mode(overwrite)

    spark = None
    try:
        spark = (
            SparkSession.builder
            .appName("MovieLensRatingAnalysis")
            .config(conf=spark_conf)
            .getOrCreate()
        )

        LOGGER.info("Reading ratings from %s/ratings.csv.", input_root)
        ratings = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(f"{input_root}/ratings.csv")

        LOGGER.info("Dataset schema:")
        ratings.printSchema()

        total_ratings = ratings.count()
        LOGGER.info("Total ratings: %s", total_ratings)

        summary = ratings.agg(
            avg("rating").alias("average_rating")
        )
        LOGGER.info("Overall average rating:")
        summary.show()

        top_users = ratings.groupBy("userId") \
            .agg(count("rating").alias("total_ratings")) \
            .orderBy(col("total_ratings").desc())
        LOGGER.info("Top 10 most active users:")
        top_users.show(10)

        LOGGER.info("Reading movies from %s/movies.csv.", input_root)
        movies = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(f"{input_root}/movies.csv")

        movie_ratings = ratings.join(
            movies, on="movieId", how="inner"
        )

        movie_stats = movie_ratings.groupBy("movieId", "title") \
            .agg(
                avg("rating").alias("avg_rating"),
                count("rating").alias("rating_count")
            )

        top_movies = movie_stats.filter(
            col("rating_count") >= 50
        ).orderBy(col("avg_rating").desc())
        LOGGER.info("Top 10 highest-rated movies (minimum 50 ratings):")
        top_movies.show(10, truncate=False)

        genre_data = movie_ratings.withColumn(
            "genre",
            explode(split(col("genres"), r"\|"))
        ).filter(col("genre") != "(no genres listed)")

        genre_stats = genre_data.groupBy("genre") \
            .agg(
                avg("rating").alias("avg_rating"),
                count("rating").alias("rating_count")
            ) \
            .orderBy(col("avg_rating").desc())
        LOGGER.info("Genre-wise average ratings:")
        genre_stats.show(20, truncate=False)

        rating_distribution = ratings.groupBy("rating") \
            .agg(
                count("rating").alias("total_ratings")
            ) \
            .orderBy(col("rating").asc())
        LOGGER.info("Rating distribution:")
        rating_distribution.show(10)

        outputs = {
            "summary": summary,
            "top_users": top_users.limit(10),
            "top_movies": top_movies.limit(100),
            "genre_stats": genre_stats,
            "rating_distribution": rating_distribution,
        }
        LOGGER.info("Writing analytics outputs to %s.", output_root)
        for name, dataframe in outputs.items():
            _write_output(
                dataframe,
                f"{output_root}/{name}",
                write_mode,
            )
    finally:
        if spark is not None:
            LOGGER.info("Stopping Spark session.")
            spark.stop()

    if mode == "cluster":
        if sync_outputs:
            _sync_hdfs_outputs_to_local(output_names, overwrite)
        else:
            LOGGER.info(
                "HDFS outputs are available under %s; local copy was disabled.",
                output_root,
            )
    else:
        LOGGER.info("All analytics results exported to %s.", output_root)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        _run_analytics()
    except Exception:
        LOGGER.exception("MovieLens analytics failed.")
        raise


if __name__ == "__main__":
    main()
