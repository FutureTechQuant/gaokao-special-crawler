import argparse

from crawlers.plans import PlanCrawler


def write_output(path: str, key: str, value):
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{key}={value}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--shard", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--github-output", default="")
    args = parser.parse_args()

    crawler = PlanCrawler()
    results = crawler.crawl(years=args.year, mode=args.mode)
    result = results[0] if results else {
        "year": args.year,
        "status": "skipped",
        "saved_documents": 0,
        "completed_schools": 0,
    }

    write_output(args.github_output, "run_year", result.get("year", args.year))
    write_output(args.github_output, "run_shard", args.shard)
    write_output(args.github_output, "run_status", result.get("status", "skipped"))
    write_output(args.github_output, "saved_documents", result.get("saved_documents", 0))
    write_output(args.github_output, "completed_schools", result.get("completed_schools", 0))

    print(result)


if __name__ == "__main__":
    main()
