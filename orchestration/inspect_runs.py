import argparse

from orchestration.pipeline_state import (
    list_recent_runs,
    get_latest_run,
)


def display_runs(limit: int):

    runs = list_recent_runs(
        limit=limit
    )

    if not runs:
        print("No pipeline runs found.")
        return

    header = (
        f"{'Season':<8}"
        f"{'Round':<8}"
        f"{'Dataset':<12}"
        f"{'Stage':<10}"
        f"{'Status':<12}"
        f"{'Attempt':<10}"
        f"{'Rows':<12}"
    )

    print(header)
    print("-" * len(header))

    for run in runs:

        print(
            f"{run['season']:<8}"
            f"{run['round_number']:<8}"
            f"{run['dataset']:<12}"
            f"{run['stage']:<10}"
            f"{run['status']:<12}"
            f"{run['attempt']:<10}"
            f"{str(run['row_count']):<12}"
        )


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--season",
        type=int,
    )

    parser.add_argument(
        "--round",
        type=int,
    )

    parser.add_argument(
        "--dataset",
        type=str,
    )

    parser.add_argument(
        "--stage",
        type=str,
        default="bronze",
    )

    args = parser.parse_args()

    if all(
        value is not None
        for value in (
            args.season,
            args.round,
            args.dataset,
        )
    ):

        result = get_latest_run(
            season=args.season,
            round_number=args.round,
            dataset=args.dataset,
            stage=args.stage,
        )

        if result is None:
            print("No matching run found.")

        else:
            for key, value in result.items():
                print(f"{key}: {value}")

    else:

        display_runs(args.limit)


if __name__ == "__main__":
    main()