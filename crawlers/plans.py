import time
import json
import os
from .base import BaseCrawler


class PlanCrawler(BaseCrawler):

    def __init__(self):
        super().__init__()
        self._first_logged = False

        # 省份ID映射（中国34个省级行政区）
        self.province_dict = {
            # 华北地区
            '11': '北京',
            '12': '天津',
            '13': '河北',
            '14': '山西',
            '15': '内蒙古',

            # 东北地区
            '21': '辽宁',
            '22': '吉林',
            '23': '黑龙江',

            # 华东地区
            '31': '上海',
            '32': '江苏',
            '33': '浙江',
            '34': '安徽',
            '35': '福建',
            '36': '江西',
            '37': '山东',

            # 华中地区
            '41': '河南',
            '42': '湖北',
            '43': '湖南',

            # 华南地区
            '44': '广东',
            '45': '广西',
            '46': '海南',

            # 西南地区
            '50': '重庆',
            '51': '四川',
            '52': '贵州',
            '53': '云南',
            '54': '西藏',

            # 西北地区
            '61': '陕西',
            '62': '甘肃',
            '63': '青海',
            '64': '宁夏',
            '65': '新疆',

            # 港澳台地区（高考数据可能不包含）
            '71': '台湾',
            '81': '香港',
            '82': '澳门',
        }

    def format_duration(self, seconds):
        """把秒数格式化为易读字符串"""
        seconds = max(0, float(seconds))
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)

        if hours > 0:
            return f"{hours}小时{minutes}分{secs}秒"
        if minutes > 0:
            return f"{minutes}分{secs}秒"
        return f"{seconds:.2f}秒"

    def get_plan_data(self, school_id, year, province_id):
        """获取指定学校、年份、省份的招生计划数据"""
        url = f"https://static-data.gaokao.cn/www/2.0/schoolspecialplan/{school_id}/{year}/{province_id}.json"

        try:
            response = self.session.get(url, timeout=10)

            if response.status_code == 200:
                result = response.json()
                if result.get('code') == '0000' and 'data' in result:
                    return result['data']
            elif response.status_code == 404:
                return 'no_data'  # 该省份无招生
        except Exception:
            pass

        return None

    def parse_years(self, years_input):
        """解析年份参数，支持多种格式"""
        if isinstance(years_input, list):
            return years_input

        if isinstance(years_input, str):
            if '-' in years_input:
                start, end = years_input.split('-')
                return [str(y) for y in range(int(start), int(end) + 1)]
            elif ',' in years_input:
                return [y.strip() for y in years_input.split(',')]
            else:
                return [years_input]

        return years_input

    def save_year_plans(self, year, plans):
        """按年份保存招生计划数据到 data/plans/{year}.json，并与已有结果合并"""
        output_dir = os.path.join('data', 'plans')
        os.makedirs(output_dir, exist_ok=True)
    
        output_path = os.path.join(output_dir, f'{year}.json')
    
        merged = []
        seen = set()
    
        if os.path.exists(output_path):
            try:
                with open(output_path, 'r', encoding='utf-8') as f:
                    old_data = json.load(f)
    
                if isinstance(old_data, list):
                    for item in old_data:
                        if isinstance(item, dict):
                            key = self.build_plan_key(item)
                            if key not in seen:
                                seen.add(key)
                                merged.append(item)
            except Exception as e:
                print(f"⚠️  读取旧年份文件失败，将覆盖重写: {e}")
    
        new_count = 0
        for item in plans:
            if not isinstance(item, dict):
                continue
            key = self.build_plan_key(item)
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            new_count += 1
    
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
    
        shard = os.getenv('PLAN_SCHOOL_SHARD', 'all')
        print(f"   💾 已保存 {year} 年招生计划: {output_path} (新增 {new_count} 条，累计 {len(merged)} 条，分片 {shard})")

    def crawl(self, school_ids=None, years=None, province_ids=None):
        """爬取招生计划数据"""
        crawl_start_time = time.perf_counter()

        print(f"\n{'='*60}")
        print("启动招生计划爬虫")
        print(f"{'='*60}\n")

        # 年份控制优先级：
        # 1. 函数参数 years
        # 2. 环境变量 PLAN_YEARS
        # 3. 默认值 ["2025", "2024", "2023"]
        if years is None:
            years_env = os.getenv('PLAN_YEARS', '2025,2024,2023')
            years = self.parse_years(years_env)
        else:
            years = self.parse_years(years)

        years = [str(y) for y in years]
        province_ids = province_ids or list(self.province_dict.keys())

        print(f"年份参数解析完成: {', '.join(years)}")
        print(f"省份数量: {len(province_ids)}")
        print(f"SAMPLE_SCHOOLS: {os.getenv('SAMPLE_SCHOOLS', '3')}")
        print()

        # 从schools.json读取学校ID
        if school_ids is None:
            load_school_start = time.perf_counter()
            print("开始读取 data/schools.json ...")

            try:
                with open('data/schools.json', 'r', encoding='utf-8') as f:
                    schools_data = json.load(f)

                    if isinstance(schools_data, list):
                        schools = schools_data
                    elif isinstance(schools_data, dict):
                        schools = schools_data.get('data', [])
                        if not schools:
                            schools = [schools_data]
                    else:
                        print(f"⚠️  schools.json 数据格式错误: {type(schools_data)}")
                        return {}

                    sample_count = int(os.getenv('SAMPLE_SCHOOLS', '3'))
                    school_ids = [
                        s['school_id']
                        for s in schools[:sample_count]
                        if isinstance(s, dict) and s.get('school_id')
                    ]

                    if not school_ids:
                        print("⚠️  未找到有效的学校ID")
                        return {}

                    load_school_elapsed = time.perf_counter() - load_school_start
                    print(f"从 schools.json 读取到 {len(school_ids)} 所学校")
                    print(f"读取 schools.json 耗时: {self.format_duration(load_school_elapsed)}")
                    print(f"前5个学校ID: {school_ids[:5]}")
                    print()

            except FileNotFoundError:
                print("⚠️  未找到 schools.json，请先运行学校爬虫")
                return {}
            except Exception as e:
                print(f"⚠️  读取 schools.json 失败: {e}")
                import traceback
                traceback.print_exc()
                return {}
        else:
            school_ids = [str(s) for s in school_ids]
            print(f"使用传入 school_ids，共 {len(school_ids)} 所学校")
            print()

        plans_by_year = {year: [] for year in years}
        school_stats = []

        print(f"{'='*60}")
        print("开始爬取招生计划")
        print(f"学校数: {len(school_ids)} | 年份: {', '.join(years)} | 省份: {len(province_ids)} 个")
        print(f"{'='*60}\n")

        for idx, school_id in enumerate(school_ids, 1):
            school_start_time = time.perf_counter()
            school_plan_count = 0
            school_request_count = 0

            print(f"\n[{idx}/{len(school_ids)}] 学校ID: {school_id}")
            print(f"   ⏱️  开始时间戳: {time.strftime('%Y-%m-%d %H:%M:%S')}")

            for year in years:
                year_start_time = time.perf_counter()
                year_count = 0
                year_request_count = 0

                for province_id in province_ids:
                    school_request_count += 1
                    year_request_count += 1

                    province_name = self.province_dict.get(province_id, f'省份{province_id}')
                    show_detail = (idx == 1 and year == years[0] and province_id == province_ids[0])

                    if show_detail:
                        print(f"\n   📡 [招生计划接口] school_id={school_id}, year={year}, province={province_name}")
                        print(f"      URL: https://static-data.gaokao.cn/www/2.0/schoolspecialplan/{school_id}/{year}/{province_id}.json")

                    data = self.get_plan_data(school_id, year, province_id)

                    if not self._first_logged and data and data != 'no_data':
                        print(f"\n      {'─'*50}")
                        print("      首次响应数据结构:")
                        print(f"      {'─'*50}")
                        print(f"      data类型: {type(data).__name__}")

                        if isinstance(data, dict):
                            print(f"      data包含键: {list(data.keys())}")

                            sample_item = None
                            for plan_type, plan_info in data.items():
                                if isinstance(plan_info, dict):
                                    items = plan_info.get('item', [])
                                    if items:
                                        sample_item = items[0]
                                        print(f"      招生类型: {plan_type}")
                                        print(f"      该类型数据条数: {len(items)}")
                                        break

                            if sample_item and isinstance(sample_item, dict):
                                fields = list(sample_item.keys())
                                print(f"\n      招生计划数据字段({len(fields)}个):")
                                print(f"      {'─'*50}")
                                for i, field in enumerate(fields, 1):
                                    value = sample_item[field]
                                    value_type = type(value).__name__
                                    if value is None:
                                        preview = "None"
                                    elif isinstance(value, str):
                                        preview = f'"{value[:25]}..."' if len(value) > 25 else f'"{value}"'
                                    elif isinstance(value, (list, dict)):
                                        preview = f"{value_type}({len(value)}项)"
                                    else:
                                        preview = str(value)
                                    print(f"      {i:2}. {field:25} = {preview}")
                                print(f"      {'─'*50}\n")

                        self._first_logged = True

                    if data == 'no_data':
                        continue
                    elif data and isinstance(data, dict):
                        for plan_type, plan_info in data.items():
                            if not isinstance(plan_info, dict):
                                continue

                            items = plan_info.get('item', [])

                            for item in items:
                                if not isinstance(item, dict):
                                    continue

                                plan_record = {
                                    'school_id': school_id,
                                    'year': year,
                                    'province_id': province_id,
                                    'province': province_name,
                                    'plan_type': plan_type,
                                    'batch': item.get('local_batch_name'),
                                    'type': item.get('type'),
                                    'major': item.get('sp_name') or item.get('spname'),
                                    'major_code': item.get('spcode'),
                                    'major_group': item.get('sg_name'),
                                    'major_group_code': item.get('sg_code'),
                                    'major_group_info': item.get('sg_info'),
                                    'level1_name': item.get('level1_name'),
                                    'level2_name': item.get('level2_name'),
                                    'level3_name': item.get('level3_name'),
                                    'plan_number': item.get('num') or item.get('plan_num'),
                                    'years': item.get('length') or item.get('years'),
                                    'tuition': item.get('tuition'),
                                    'note': item.get('note') or item.get('remark'),
                                }
                                plans_by_year[year].append(plan_record)
                                year_count += 1
                                school_plan_count += 1

                    if show_detail:
                        print(f"      ✓ {province_name}: 获取数据")

                year_elapsed = time.perf_counter() - year_start_time
                if year_count > 0:
                    print(
                        f"   ✓ {year}年: 获取 {year_count} 条招生计划 | "
                        f"请求 {year_request_count} 次 | 耗时 {self.format_duration(year_elapsed)}"
                    )
                else:
                    print(
                        f"   ⚠️  {year}年: 无招生计划数据 | "
                        f"请求 {year_request_count} 次 | 耗时 {self.format_duration(year_elapsed)}"
                    )

            school_elapsed = time.perf_counter() - school_start_time
            school_stats.append({
                'school_id': school_id,
                'plans': school_plan_count,
                'requests': school_request_count,
                'elapsed_seconds': school_elapsed,
            })

            if school_plan_count > 0:
                print(
                    f"   ✅ 学校ID {school_id}：共 {school_plan_count} 条招生计划 | "
                    f"请求 {school_request_count} 次 | 总耗时 {self.format_duration(school_elapsed)}"
                )
            else:
                print(
                    f"   ⚠️  学校ID {school_id}：无招生计划数据 | "
                    f"请求 {school_request_count} 次 | 总耗时 {self.format_duration(school_elapsed)}"
                )

            avg_per_request = school_elapsed / school_request_count if school_request_count else 0
            print(f"   📊 单请求平均耗时: {avg_per_request:.2f} 秒")

        print(f"\n{'='*60}")
        print("✅ 招生计划爬取完成！")

        total_count = 0
        for year in sorted(plans_by_year.keys(), reverse=True):
            year_plans = plans_by_year[year]
            self.save_year_plans(year, year_plans)
            total_count += len(year_plans)

            if year_plans:
                provinces = set(p.get('province') for p in year_plans if p.get('province'))
                total_enrollment = sum(
                    int(p.get('plan_number', 0) or 0)
                    for p in year_plans
                    if p.get('plan_number')
                )
                print(f"   {year}年: {len(year_plans)} 条 | 覆盖省份: {len(provinces)} 个 | 总招生人数: {total_enrollment}")
            else:
                print(f"   {year}年: 0 条")

        total_elapsed = time.perf_counter() - crawl_start_time
        print(f"   总计: {total_count} 条招生计划")
        print(f"   总耗时: {self.format_duration(total_elapsed)}")

        if school_stats:
            school_stats_sorted = sorted(
                school_stats,
                key=lambda x: x['elapsed_seconds'],
                reverse=True
            )

            print("\n最慢的前10所学校:")
            for i, stat in enumerate(school_stats_sorted[:10], 1):
                print(
                    f"   {i:2}. 学校ID {stat['school_id']} | "
                    f"耗时 {self.format_duration(stat['elapsed_seconds'])} | "
                    f"请求 {stat['requests']} 次 | "
                    f"计划 {stat['plans']} 条"
                )

        print(f"{'='*60}\n")

        return plans_by_year

    def build_plan_key(self, item):
        return (
            str(item.get('school_id') or ''),
            str(item.get('year') or ''),
            str(item.get('province_id') or ''),
            str(item.get('plan_type') or ''),
            str(item.get('batch') or ''),
            str(item.get('type') or ''),
            str(item.get('major') or ''),
            str(item.get('major_code') or ''),
            str(item.get('major_group_code') or ''),
        )


if __name__ == "__main__":
    import sys

    years_arg = sys.argv[1] if len(sys.argv) > 1 else None

    crawler = PlanCrawler()
    crawler.crawl(years=years_arg)
