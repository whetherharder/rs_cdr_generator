#!/usr/bin/env python3
"""
Сверка графа контактов между питоновским и rust-генераторами CDR.

Читает выгрузки обоих генераторов (дерево <ne_id>/CDR_<ne_id>_<date>.csv.gz,
формат общий для обеих версий — см. docs/field-mapping.md) и строит граф
"кто с кем взаимодействовал" ОТДЕЛЬНО по каждому типу трафика
(mo_call/mt_call, mo_sms/mt_sms, sgw_data/pgw_data) и суммарно по CALL+SMS
(запись — пара calling/called; DATA пары не образует, см. ниже).

Раздельный счёт введён 2026-09-08: SMS-трафик асимметричнее голосового
(рассылки, сервисные номера, односторонние цепочки), и доля взаимных пар
вместе с распределением веса сдвигаются у CALL и SMS неодинаково — суммарная
картина «CALL+SMS вместе» это скрывает.

DATA (sgw_data/pgw_data) в графовые метрики (вес пары, взаимность, top-5,
собеседники) не попадает намеренно: у data-сессии нет второй стороны
(`called_number` пуст, см. docs/field-mapping.md) — считаются только записи.

Запуск:
    python3 tools/graph-compare.py <python_out_dir> <rust_out_dir> [--half-day YYYY-MM-DD]

--half-day задаёт границу для проверки устойчивости top-5 (первая половина
периода / вторая); по умолчанию — медианная дата прогона.
"""
import argparse
import csv
import gzip
import sys
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, median


# Группы типов записи, по которым считаются графовые метрики раздельно,
# плюс "all_pairs" — суммарно по CALL+SMS (у DATA пары нет, см. докстринг).
TYPE_GROUPS = {
    "call": {"mo_call", "mt_call"},
    "sms": {"mo_sms", "mt_sms"},
    "data": {"sgw_data", "pgw_data"},
}
PAIR_TYPES = TYPE_GROUPS["call"] | TYPE_GROUPS["sms"]


def iter_records(root: Path):
    """Идёт по всем CDR_*.csv.gz под root и отдаёт (record_type, calling, called, date)."""
    for f in sorted(root.rglob("CDR_*.csv.gz")):
        with gzip.open(f, "rt", newline="") as fh:
            first = fh.readline()
            if first.startswith("#"):
                header_line = fh.readline()
            else:
                header_line = first
            header = next(csv.reader([header_line]))
            idx = {name: i for i, name in enumerate(header)}
            reader = csv.reader(fh)
            i_type = idx["record_type"]
            i_call = idx["calling_number"]
            i_cld = idx["called_number"]
            i_ts = idx["event_timestamp"]
            i_served = idx["served_msisdn"]
            for row in reader:
                if not row:
                    continue
                yield row[i_type], row[i_call], row[i_cld], row[i_ts][:10], row[i_served]


def record_type_counts(root: Path):
    """Число записей КАЖДОГО record_type, без фильтра по типу или направлению —
    для таблицы «сколько записей какого типа» в отчёте."""
    counts = Counter()
    for rtype, _calling, _called, _date, _served in iter_records(root):
        counts[rtype] += 1
    return counts


def build_graph(root: Path, type_filter: set):
    """Возвращает (served_set, directed_counter[(a,b)] = n, отсортированные даты, row_n)
    по записям, чей record_type входит в type_filter. served — по ВСЕМ записям
    независимо от type_filter (граница «кто абонент» не должна зависеть от того,
    какой тип трафика сейчас считаем)."""
    served = set()
    directed = Counter()
    dates = set()
    row_n = 0
    for rtype, calling, called, date, served_msisdn in iter_records(root):
        served.add(served_msisdn)
        dates.add(date)
        if rtype not in type_filter:
            continue
        if not calling or not called:
            continue
        directed[(calling, called)] += 1
        row_n += 1
    return served, directed, sorted(dates), row_n


def restrict_internal(directed: Counter, served: set):
    """Оставляет только рёбра, у которых обе стороны — известные served_msisdn (внутренние абоненты)."""
    return Counter({k: v for k, v in directed.items() if k[0] in served and k[1] in served})


def pair_weights(directed_internal: Counter):
    """Неориентированный вес пары = сумма звонков в обе стороны."""
    undirected = Counter()
    for (a, b), n in directed_internal.items():
        key = (a, b) if a <= b else (b, a)
        undirected[key] += n
    return undirected


def weight_distribution(undirected: Counter):
    weights = list(undirected.values())
    if not weights:
        return None
    n = len(weights)
    buckets = {"1": 0, "2-5": 0, "6-20": 0, "20+": 0}
    for w in weights:
        if w == 1:
            buckets["1"] += 1
        elif w <= 5:
            buckets["2-5"] += 1
        elif w <= 20:
            buckets["6-20"] += 1
        else:
            buckets["20+"] += 1
    return {
        "pairs": n,
        "median": median(weights),
        "mean": round(mean(weights), 2),
        "max": max(weights),
        "share_1": round(buckets["1"] / n, 3),
        "share_2_5": round(buckets["2-5"] / n, 3),
        "share_6_20": round(buckets["6-20"] / n, 3),
        "share_20p": round(buckets["20+"] / n, 3),
    }


def mutual_share(directed_internal: Counter):
    pairs = set(directed_internal.keys())
    total_unordered = set()
    mutual = 0
    seen = set()
    for (a, b) in pairs:
        key = (a, b) if a <= b else (b, a)
        if key in seen:
            continue
        seen.add(key)
        total_unordered.add(key)
        if (a, b) in pairs and (b, a) in pairs:
            mutual += 1
    if not total_unordered:
        return None
    return round(mutual / len(total_unordered), 3), len(total_unordered)


def contacts_per_subscriber(undirected: Counter):
    per_sub = defaultdict(set)
    for (a, b) in undirected.keys():
        per_sub[a].add(b)
        per_sub[b].add(a)
    counts = [len(v) for v in per_sub.values()]
    if not counts:
        return None
    return {
        "subscribers_with_contacts": len(counts),
        "min": min(counts),
        "median": median(counts),
        "mean": round(mean(counts), 2),
        "max": max(counts),
    }


def top5_stability(root: Path, served: set, dates: list, half_day: str | None, type_filter: set):
    """Top-5 контактов по весу в первой и второй половине периода, средний Jaccard."""
    if not dates:
        return None
    if half_day is None:
        half_day = dates[len(dates) // 2]
    half1 = Counter()
    half2 = Counter()
    for rtype, calling, called, date, served_msisdn in iter_records(root):
        if rtype not in type_filter or not calling or not called:
            continue
        if calling not in served or called not in served:
            continue
        key = (calling, called) if calling <= called else (called, calling)
        if date < half_day:
            half1[key] += 1
        else:
            half2[key] += 1

    def top5_map(counter):
        per_sub = defaultdict(Counter)
        for (a, b), w in counter.items():
            per_sub[a][b] += w
            per_sub[b][a] += w
        return {
            sub: [c for c, _ in cnt.most_common(5)]
            for sub, cnt in per_sub.items()
        }

    t1 = top5_map(half1)
    t2 = top5_map(half2)
    common_subs = set(t1) & set(t2)
    jaccards = []
    for s in common_subs:
        a, b = set(t1[s]), set(t2[s])
        if not a and not b:
            continue
        j = len(a & b) / len(a | b) if (a | b) else 0.0
        jaccards.append(j)
    if not jaccards:
        return None
    return {
        "half_boundary": half_day,
        "subscribers_compared": len(jaccards),
        "mean_jaccard": round(mean(jaccards), 3),
        "median_jaccard": round(median(jaccards), 3),
    }


def resolve_known(root: Path, known_subs_file: Path | None, served_from_all: set):
    if known_subs_file is None:
        return served_from_all, False
    # served_msisdn у питона загрязнён внешними номерами (см. отчёт задачи 1:
    # даже при anomalies.enabled=false туда попадают +1212/... из external_numbers) —
    # берём границу "кто внутренний абонент" из явного списка реальных MSISDN
    # (subscribers.json ассетов), а не из значений поля served_msisdn в CSV.
    known = set(known_subs_file.read_text().split())
    return known, True


def analyze_group(group_name: str, type_filter: set, root: Path, half_day: str | None, served: set):
    """Метрики графа для одной группы типов записи на уже разрешённом served-множестве."""
    _served_ignored, directed, dates, row_n = build_graph(root, type_filter)
    directed_internal = restrict_internal(directed, served)
    undirected = pair_weights(directed_internal)

    wd = weight_distribution(undirected)
    ms = mutual_share(directed_internal)
    cps = contacts_per_subscriber(undirected)
    stab = top5_stability(root, served, dates, half_day, type_filter)

    print(f"  -- {group_name} ({sorted(type_filter)}) --")
    print(f"  записей группы всего: {row_n}, из них внутренних (обе стороны — известный абонент): "
          f"{sum(directed_internal.values())}")
    print(f"  распределение веса пары: {wd}")
    print(f"  доля взаимных пар: {ms}")
    print(f"  уникальных собеседников на абонента: {cps}")
    print(f"  устойчивость top-5 между половинами периода: {stab}")

    return {
        "row_n": row_n,
        "weight_distribution": wd,
        "mutual_share": ms,
        "contacts_per_subscriber": cps,
        "top5_stability": stab,
    }


def analyze(label: str, root: Path, half_day: str | None, known_subs_file: Path | None = None,
            groups: dict | None = None):
    if groups is None:
        groups = dict(TYPE_GROUPS, all_pairs=PAIR_TYPES)

    # served_set берём по ВСЕМ записям (build_graph с type_filter=множество всех
    # встреченных типов даёт то же самое, что и старое поведение "по всем строкам").
    all_types = set().union(*TYPE_GROUPS.values())
    served_raw, _directed, dates, _row_n = build_graph(root, all_types)
    served, used_known = resolve_known(root, known_subs_file, served_raw)

    print(f"\n=== {label} ({root}) ===")
    if used_known:
        print(f"известных абонентов из явного списка: {len(served)} (вместо {len(served_raw)} по served_msisdn)")
    else:
        print(f"served_msisdn (внутренние абоненты, встреченные хоть раз): {len(served)}")
    print(f"дат в выгрузке: {len(dates)} ({dates[0]}..{dates[-1]})" if dates else "дат нет")

    counts = record_type_counts(root)
    total = sum(counts.values())
    print(f"записей по типам (всего {total}):")
    for rt in sorted(counts):
        share = round(100 * counts[rt] / total, 1) if total else 0.0
        print(f"  {rt}: {counts[rt]} ({share}%)")

    result = {"served": len(served), "record_type_counts": dict(counts), "groups": {}}
    for gname, gtypes in groups.items():
        result["groups"][gname] = analyze_group(gname, gtypes, root, half_day, served)
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("python_dir", type=Path)
    ap.add_argument("rust_dir", type=Path)
    ap.add_argument("--half-day", default=None, help="Граница половин периода, YYYY-MM-DD")
    ap.add_argument("--known-subs-python", type=Path, default=None,
                     help="Явный список реальных MSISDN питона (см. предупреждение о served_msisdn)")
    ap.add_argument("--known-subs-rust", type=Path, default=None)
    ap.add_argument("--only-group", choices=sorted(TYPE_GROUPS) + ["all_pairs"], default=None,
                     help="Считать только одну группу типов (для точечных прогонов, напр. динамики по периоду)")
    args = ap.parse_args()

    groups = dict(TYPE_GROUPS, all_pairs=PAIR_TYPES)
    if args.only_group:
        groups = {args.only_group: groups[args.only_group]}

    analyze("python (upstream)", args.python_dir, args.half_day, args.known_subs_python, groups)
    analyze("rust", args.rust_dir, args.half_day, args.known_subs_rust, groups)


if __name__ == "__main__":
    main()
