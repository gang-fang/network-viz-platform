#!/usr/bin/env python3

"""
Map UniProt proteome/reference proteome IDs (UPIDs) to NCBI taxonomy IDs.

This version is optimized for large input files. It queries the UniProt
Proteomes search endpoint in batches instead of making one HTTP request per UPID.

Input:
    A plain-text file with one UniProt proteome ID per line, for example:
        UP000000556
        UP000005640

Output:
    By default, a two-column CSV file:
        upid,taxid
        UP000000556,160488
        UP000005640,9606

Usage:
    python3 upid_to_taxid.py upids.txt upid_to_taxid.csv

Optional:
    python3 upid_to_taxid.py upids.txt upid_to_taxid.csv --batch-size 100
    python3 upid_to_taxid.py upids.txt upid_to_taxid.tsv --tsv
    python3 upid_to_taxid.py upids.txt upid_to_taxid.csv --no-header
"""

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ENTRY_BASE = "https://rest.uniprot.org/proteomes"
SEARCH_BASE = "https://rest.uniprot.org/proteomes/search"

UPID_RE = re.compile(r"^UP\d+$", re.IGNORECASE)


def get_nested(d, paths, default=""):
    """Return the first non-empty value found among several nested JSON paths."""
    for path in paths:
        cur = d
        ok = True
        for key in path:
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return default


def clean_value(x):
    if x is None:
        return ""
    if isinstance(x, (list, tuple)):
        return ";".join(str(i) for i in x)
    if isinstance(x, dict):
        return json.dumps(x, ensure_ascii=False)
    return str(x)


def read_upids(path):
    """Read one UPID per line; ignore blank lines and comment lines."""
    upids = []
    seen = set()

    with open(path) as handle:
        for line_no, line in enumerate(handle, start=1):
            value = line.strip()
            if not value or value.startswith("#"):
                continue

            # If a line has extra whitespace-separated columns, use the first token.
            upid = value.split()[0].strip().upper()

            if not UPID_RE.match(upid):
                sys.stderr.write(
                    f"Warning: line {line_no}: '{upid}' does not look like a UniProt proteome ID. "
                    "It will still be queried.\n"
                )

            if upid not in seen:
                upids.append(upid)
                seen.add(upid)

    return upids


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_json(url, user_agent):
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def extract_upid(data):
    """Extract the UniProt proteome ID from one Proteomes JSON record."""
    return clean_value(
        data.get("id")
        or data.get("upid")
        or data.get("proteomeId")
        or get_nested(data, [["proteome", "id"], ["proteome", "upid"]])
    ).upper()


def extract_taxid(data):
    """Extract NCBI taxonomy ID from one Proteomes JSON record."""
    taxid = get_nested(data, [
        ["taxonomy", "taxonId"],
        ["organism", "taxonId"],
        ["taxonId"],
    ])
    return clean_value(taxid)


def query_direct_one(upid, user_agent):
    """Direct single-UPID fallback. Used only for records missing from batch search."""
    url = f"{ENTRY_BASE}/{urllib.parse.quote(upid)}?format=json"
    data = fetch_json(url, user_agent)
    taxid = extract_taxid(data)
    return taxid if taxid else "NOT_FOUND"


def query_search_batch(upids, user_agent):
    """
    Query several UPIDs at once using the UniProt Proteomes search endpoint.

    Returns a dictionary: {UPID: taxid} for entries found in the response.
    Missing UPIDs are handled by the caller.
    """
    query = " OR ".join(f"(upid:{upid})" for upid in upids)
    params = {
        "query": query,
        "format": "json",
        "size": str(max(len(upids), 1)),
    }
    url = SEARCH_BASE + "?" + urllib.parse.urlencode(params)
    data = fetch_json(url, user_agent)

    out = {}
    for item in data.get("results", []):
        upid = extract_upid(item)
        taxid = extract_taxid(item)
        if upid and taxid:
            out[upid] = taxid

    return out


def query_batch_with_split(upids, user_agent):
    """
    Query a batch. If the query URL is too long or the server rejects it,
    split the batch and retry smaller chunks.
    """
    try:
        return query_search_batch(upids, user_agent)
    except urllib.error.HTTPError as e:
        # 400/414 commonly indicate a query problem or URL too long.
        # Splitting usually fixes that for large OR queries.
        if e.code in (400, 414) and len(upids) > 1:
            mid = len(upids) // 2
            left = query_batch_with_split(upids[:mid], user_agent)
            right = query_batch_with_split(upids[mid:], user_agent)
            left.update(right)
            return left
        body = e.read().decode("utf-8", errors="replace").replace("\n", " ")
        sys.stderr.write(f"Warning: batch query failed with HTTP {e.code}: {body[:300]}\n")
        return {}
    except Exception as e:
        sys.stderr.write(f"Warning: batch query failed: {e}\n")
        return {}


def main():
    parser = argparse.ArgumentParser(
        description="Map UniProt proteome/reference proteome IDs to NCBI taxonomy IDs."
    )
    parser.add_argument("input", help="Input text file with one UniProt proteome ID per line.")
    parser.add_argument("output", help="Output two-column mapping file. Default format: CSV with columns upid,taxid.")
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Do not write the header row. Default: write header row.",
    )
    parser.add_argument(
        "--tsv",
        action="store_true",
        help="Write tab-separated output instead of the default comma-separated CSV output.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of UPIDs to query per UniProt request. Default: 100.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Seconds to sleep between batch requests. Default: 0.2.",
    )
    parser.add_argument(
        "--no-direct-fallback",
        action="store_true",
        help="Do not use slower one-by-one direct lookup for UPIDs missing from batch search.",
    )
    parser.add_argument(
        "--user-agent",
        default="upid-to-taxid-batch/1.0",
        help="User-Agent string sent to UniProt. Default: upid-to-taxid-batch/1.0.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress progress messages.",
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        sys.stderr.write("Error: --batch-size must be at least 1.\n")
        sys.exit(1)

    upids = read_upids(args.input)
    if not upids:
        sys.stderr.write(f"Error: no UniProt proteome IDs found in {args.input}\n")
        sys.exit(1)

    results = {upid: "NOT_FOUND" for upid in upids}

    total_batches = (len(upids) + args.batch_size - 1) // args.batch_size
    for batch_no, batch in enumerate(chunks(upids, args.batch_size), start=1):
        if not args.quiet:
            sys.stderr.write(f"Querying batch {batch_no}/{total_batches} ({len(batch)} UPIDs)...\n")

        batch_results = query_batch_with_split(batch, args.user_agent)
        for upid, taxid in batch_results.items():
            if upid in results:
                results[upid] = taxid

        time.sleep(args.sleep)

    if not args.no_direct_fallback:
        missing = [upid for upid in upids if results[upid] == "NOT_FOUND"]
        if missing and not args.quiet:
            sys.stderr.write(f"Direct fallback for {len(missing)} UPIDs not found by batch search...\n")

        for upid in missing:
            try:
                results[upid] = query_direct_one(upid, args.user_agent)
            except urllib.error.HTTPError as e:
                sys.stderr.write(f"Warning: {upid}: direct lookup HTTP {e.code}\n")
                results[upid] = "ERROR"
            except Exception as e:
                sys.stderr.write(f"Warning: {upid}: direct lookup failed: {e}\n")
                results[upid] = "ERROR"
            time.sleep(args.sleep)

    delimiter = "\t" if args.tsv else ","

    with open(args.output, "w", newline="") as out:
        writer = csv.writer(out, delimiter=delimiter, lineterminator="\n")

        if not args.no_header:
            writer.writerow(["upid", "taxid"])

        for upid in upids:
            writer.writerow([upid, results[upid]])

    print(f"Done. Output written to: {args.output}")


if __name__ == "__main__":
    main()
