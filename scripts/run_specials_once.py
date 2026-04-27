import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from crawlers.specials import SpecialCrawler


def write_output(path: str, key: str, value):
    if not path:
        return
    with open(path, 'a', encoding='utf-8') as f:
        f.write(f'{key}={value}')


def load_schools():
    schools_file = PROJECT_ROOT / os.getenv('SCHOOL_DATA_FILE', 'data/schools.json')
    if not schools_file.exists():
        raise FileNotFoundError(f'未找到学校文件: {schools_file}')

    with open(schools_file, 'r', encoding='utf-8') as f:
        payload = json.load(f)

    if isinstance(payload, list):
        schools = payload
    elif isinstance(payload, dict):
        schools = payload.get('data', [])
        if not schools and payload.get('school_id'):
            schools = [payload]
    else:
        schools = []

    items = []
    for item in schools:
        if not isinstance(item, dict) or not item.get('school_id'):
            continue
        items.append({
            'school_id': str(item.get('school_id')),
            'school_name': item.get('name') or item.get('school_name') or item.get('school_name_cn') or '',
        })

    def sort_key(x):
        sid = x['school_id']
        return (0, int(sid)) if sid.isdigit() else (1, sid)

    return sorted({item['school_id']: item for item in items}.values(), key=sort_key)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--github-output', default='')
    args = parser.parse_args()

    schools = load_schools()

    if not schools:
        write_output(args.github_output, 'run_status', 'skipped')
        write_output(args.github_output, 'saved_documents', 0)
        write_output(args.github_output, 'completed_schools', 0)
        print({'status': 'skipped'})
        return

    crawler = SpecialCrawler()
    result = crawler.crawl(schools=schools)

    write_output(args.github_output, 'run_status', result.get('status', 'skipped'))
    write_output(args.github_output, 'saved_documents', result.get('saved_documents', 0))
    write_output(args.github_output, 'completed_schools', result.get('completed_schools', 0))

    print(result)


if __name__ == '__main__':
    main()
