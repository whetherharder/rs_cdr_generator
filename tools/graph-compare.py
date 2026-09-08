#!/usr/bin/env python3
"""
Сверка графа контактов между питоновским и rust-генераторами CDR.

Читает выгрузки обоих генераторов (дерево <ne_id>/CDR_<ne_id>_<date>.csv.gz,
формат общий для обеих версий — см. docs/field-mapping.md) и строит граф
"звонков между абонентами" по записям record_type in {mo_call, mt_call}.
SMS исключены из сравнения намеренно: в эталонном прогоне upstream-генератор
не выдал ни одной SMS-записи при ненулевых лямбдах в конфиге (см. отчёт
задачи 1) — сравнивать по SMS означало бы сравнивать пустое с непустым.

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


CALL_TYPES = {"mo_call", "mt_call"}


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


def build_graph(root: Path):
    """Возвращает (served_set, directed_counter[(a,b)] = n, per_day_pair_counter)."""
    served = set()
    directed = Counter()
    per_half = {"1": Counter(), "2": Counter()}
    dates = set()
    row_n = 0
    for rtype, calling, called, date, served_msisdn in iter_records(root):
        served.add(served_msisdn)
        dates.add(date)
        if rtype not in CALL_TYPES:
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


def top5_stability(root: Path, served: set, dates: list, half_day: str | None):
    """Top-5 контактов по весу в первой и второй половине периода, средний Jaccard."""
    if not dates:
        return None
    if half_day is None:
        half_day = dates[len(dates) // 2]
    half1 = Counter()
    half2 = Counter()
    for rtype, calling, called, date, served_msisdn in iter_records(root):
        if rtype not in CALL_TYPES or not calling or not called:
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


def analyze(label: str, root: Path, half_day: str | None, known_subs_file: Path | None = None):
    served, directed, dates, row_n = build_graph(root)
    if known_subs_file is not None:
        # served_msisdn у питона загрязнён внешними номерами (см. отчёт задачи 1:
        # даже при anomalies.enabled=false туда попадают +1212/... из external_numbers) —
        # берём границу "кто внутренний абонент" из явного списка реальных MSISDN
        # (subscribers.json ассетов), а не из значений поля served_msisdn в CSV.
        known = set(known_subs_file.read_text().split())
        print(f"известных абонентов из явного списка: {len(known)} (вместо {len(served)} по served_msisdn)")
        served = known
    directed_internal = restrict_internal(directed, served)
    undirected = pair_weights(directed_internal)

    print(f"\n=== {label} ({root}) ===")
    print(f"served_msisdn (внутренние абоненты, встреченные хоть раз): {len(served)}")
    print(f"дат в выгрузке: {len(dates)} ({dates[0]}..{dates[-1]})" if dates else "дат нет")
    print(f"call-записей всего: {row_n}, из них внутренних (обе стороны — известный абонент): "
          f"{sum(directed_internal.values())}")

    wd = weight_distribution(undirected)
    print(f"распределение веса пары: {wd}")

    ms = mutual_share(directed_internal)
    print(f"доля взаимных пар: {ms}")

    cps = contacts_per_subscriber(undirected)
    print(f"уникальных собеседников на абонента: {cps}")

    stab = top5_stability(root, served, dates, half_day)
    print(f"устойчивость top-5 между половинами периода: {stab}")

    return {
        "served": len(served),
        "weight_distribution": wd,
        "mutual_share": ms,
        "contacts_per_subscriber": cps,
        "top5_stability": stab,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("python_dir", type=Path)
    ap.add_argument("rust_dir", type=Path)
    ap.add_argument("--half-day", default=None, help="Граница половин периода, YYYY-MM-DD")
    ap.add_argument("--known-subs-python", type=Path, default=None,
                     help="Явный список реальных MSISDN питона (см. предупреждение о served_msisdn)")
    ap.add_argument("--known-subs-rust", type=Path, default=None)
    args = ap.parse_args()

    analyze("python (upstream)", args.python_dir, args.half_day, args.known_subs_python)
    analyze("rust", args.rust_dir, args.half_day, args.known_subs_rust)


if __name__ == "__main__":
    main()
