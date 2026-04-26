import gzip
import hashlib
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

import requests

from .base import BaseCrawler


class PlanCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()

        self._first_logged = False
        self._first_log_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._local = threading.local()

        self.state_file = Path("data/plans_progress.json")
        self.manifest_file = Path("data/manifest.json")
        self.chunk_dir = Path("data/chunks")

        self.max_workers = max(1, int(os.getenv("PLAN_MAX_WORKERS", "6")))
        self.flush_every = max(20, int(os.getenv("PLAN_FLUSH_EVERY", "200")))
        self.chunk_record_limit = max(
            1000, int(os.getenv("PLAN_CHUNK_RECORD_LIMIT", "200000"))
        )
        self.time_limit_seconds = int(
            os.getenv("PLAN_TIME_LIMIT_SECONDS", str(5 * 60 * 60 - 15 * 60))
        )  # 默认 4小时45分钟
        self.start_time = time.time()

        self.completed_keys = set()
        self.chunk_files = []
        self.current_chunk_index = 0
        self.current_chunk_records = 0
        self.current_chunk_file = None
        self.total_records = 0
        self.scope_signature = None
        self.scope_name = None

        self.province_dict = {
            "11": "北京",
            "12": "天津",
            "13": "河北",
            "14": "山西",
            "15": "内蒙古",
            "21": "辽宁",
            "22": "吉林",
            "23": "黑龙江",
            "31": "上海",
            "32": "江苏",
            "33": "浙江",
            "34": "安徽",
            "35": "福建",
            "36": "江西",
            "37": "山东",
            "41": "河南",
            "42": "湖北",
            "43": "湖南",
            "44": "广东",
            "45": "广西",
            "46": "海南",
            "50": "重庆",
            "51": "四川",
            "52": "贵州",
            "53": "云南",
            "54": "西藏",
            "61": "陕西",
            "62": "甘肃",
            "63": "青海",
            "64": "宁夏",
            "65": "新疆",
            "71": "台湾",
            "81": "香港",
            "82": "澳门",
        }

    def get_thread_session(self):
        if not hasattr(self._local, "session"):
            session = requests.Session()
            session.headers.update(self.headers)
            self._local.session = session
        return self._local.session

    def parse_years(self, years_input):
        if isinstance(years_input, list):
            return [str(y).strip() for y in years_input if str(y).strip()]

        if years_input is None:
            return []

        if isinstance(years_input, (int, float)):
            return [str(int(years_input))]

        years_input = str(years_input).strip()
        if not years_input:
            return []

        if "-" in years_input:
            start, end = [x.strip() for x in years_input.split("-", 1)]
            start_year, end_year = int(start), int(end)
            step = -1 if start_year > end_year else 1
            return [str(y) for y in range(start_year, end_year + step, step)]

        if "," in years_input:
            return [y.strip() for y in years_input.split(",") if y.strip()]

        return [years_input]

    def load_school_ids(self, school_ids=None):
        if school_ids is not None:
            return [str(sid) for sid in school_ids]

        with open("data/schools.json", "r", encoding="utf-8") as f:
            schools_data = json.load(f)

        if isinstance(schools_data, list):
            schools = schools_data
        elif isinstance(schools_data, dict):
            schools = schools_data.get("data", []) or [schools_data]
        else:
            raise ValueError(f"schools.json 数据格式错误: {type(schools_data)}")

        sample_count = int(os.getenv("SAMPLE_SCHOOLS", "0") or 0)
        if sample_count > 0:
            schools = schools[:sample_count]

        school_ids = [
            str(s["school_id"])
            for s in schools
            if isinstance(s, dict) and s.get("school_id")
        ]

        if not school_ids:
            raise ValueError("未找到有效的学校ID")

        return school_ids

    def build_scope_signature(self, school_ids, years, province_ids):
        raw = json.dumps(
            {
                "school_ids": school_ids,
                "years": years,
                "province_ids": province_ids,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def build_scope_name(self, years):
        joined = "-".join(years)
        return joined[:120]

    def task_key(self, school_id, year, province_id):
        return f"{school_id}_{year}_{province_id}"

    def build_tasks(self, school_ids, years, province_ids, completed_keys=None):
        completed_keys = completed_keys or set()
        tasks = []

        for school_id in school_ids:
            for year in years:
                for province_id in province_ids:
                    key = self.task_key(school_id, year, province_id)
                    if key not in completed_keys:
                        tasks.append((str(school_id), str(year), str(province_id)))

        return tasks

    def get_plan_data(self, school_id, year, province_id):
        url = f"https://static-data.gaokao.cn/www/2.0/schoolspecialplan/{school_id}/{year}/{province_id}.json"
        session = self.get_thread_session()

        try:
            response = session.get(url, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if result.get("code") == "0000" and "data" in result:
                    return result["data"]
            elif response.status_code == 404:
                return "no_data"
        except Exception:
            return None

        return None

    def load_progress(self):
        if not self.state_file.exists():
            return None

        with open(self.state_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def reset_scope_files(self):
        if self.chunk_dir.exists():
            for file in self.chunk_dir.glob("*.jsonl.gz"):
                file.unlink()

        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_files = []
        self.current_chunk_index = 0
        self.current_chunk_records = 0
        self.current_chunk_file = None
        self.total_records = 0

    def restore_progress(self, progress):
        self.completed_keys = set(progress.get("completed", []))
        self.chunk_files = progress.get("chunk_files", [])
        self.current_chunk_index = int(progress.get("current_chunk_index", 0) or 0)
        self.current_chunk_records = int(progress.get("current_chunk_records", 0) or 0)
        self.total_records = int(progress.get("total_records", 0) or 0)

        current_name = progress.get("current_chunk")
        self.current_chunk_file = self.chunk_dir / current_name if current_name else None
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

    def next_chunk_path(self):
        self.current_chunk_index += 1
        filename = f"plans-{self.scope_name}-{self.current_chunk_index:05d}.jsonl.gz"

        if filename not in self.chunk_files:
            self.chunk_files.append(filename)

        self.current_chunk_file = self.chunk_dir / filename
        self.current_chunk_records = 0
        return self.current_chunk_file

    def ensure_chunk_file(self):
        self.chunk_dir.mkdir(parents=True, exist_ok=True)

        if self.current_chunk_file is None:
            return self.next_chunk_path()

        if self.current_chunk_records >= self.chunk_record_limit:
            return self.next_chunk_path()

        return self.current_chunk_file

    def append_records(self, records):
        if not records:
            return

        with self._write_lock:
            for record in records:
                chunk_path = self.ensure_chunk_file()
                with gzip.open(chunk_path, "at", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                self.current_chunk_records += 1
                self.total_records += 1

    def save_manifest(self, school_ids, years, province_ids, meta):
        payload = {
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scope_signature": self.scope_signature,
            "scope_name": self.scope_name,
            "years": years,
            "school_count": len(school_ids),
            "province_count": len(province_ids),
            "total_records": self.total_records,
            "chunk_record_limit": self.chunk_record_limit,
            "chunk_files": self.chunk_files,
            "meta": meta,
        }
        with open(self.manifest_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def save_progress(self, school_ids, years, province_ids, meta):
        payload = {
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scope_signature": self.scope_signature,
            "scope_name": self.scope_name,
            "completed_count": len(self.completed_keys),
            "completed": sorted(self.completed_keys),
            "chunk_dir": str(self.chunk_dir),
            "chunk_files": self.chunk_files,
            "current_chunk": self.current_chunk_file.name if self.current_chunk_file else None,
            "current_chunk_index": self.current_chunk_index,
            "current_chunk_records": self.current_chunk_records,
            "total_records": self.total_records,
            "meta": meta,
        }

        with self._state_lock:
            with open(self.state_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

        self.save_manifest(school_ids, years, province_ids, meta)

    def should_stop(self):
        return (time.time() - self.start_time) >= self.time_limit_seconds

    def parse_plan_records(self, school_id, year, province_id, data):
        province_name = self.province_dict.get(province_id, f"省份{province_id}")
        records = []

        if not isinstance(data, dict):
            return records

        with self._first_log_lock:
            if 
