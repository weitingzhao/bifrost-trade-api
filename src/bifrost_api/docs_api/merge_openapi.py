"""Fetch and merge multiple OpenAPI 3.x specs into one (stdlib only, no httpx)."""

from __future__ import annotations

import copy
import json
import logging
import sys
from typing import Any, Dict, List
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_FETCH_TIMEOUT = 10


def fetch_openapi(url: str, *, timeout: int = _FETCH_TIMEOUT) -> Dict[str, Any]:
    """GET an OpenAPI JSON spec from *url*. Raises on non-200 or invalid JSON."""
    req = Request(url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return json.loads(body)


def _rewrite_refs(obj: Any, old_prefix: str, new_prefix: str) -> Any:
    """Recursively rewrite ``$ref`` strings that start with *old_prefix*."""
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if k == "$ref" and isinstance(v, str) and v.startswith(old_prefix):
                suffix = v[len(old_prefix):]
                out[k] = new_prefix + suffix
            else:
                out[k] = _rewrite_refs(v, old_prefix, new_prefix)
        return out
    if isinstance(obj, list):
        return [_rewrite_refs(item, old_prefix, new_prefix) for item in obj]
    return obj


def _prefix_components(
    components: Dict[str, Any],
    prefix: str,
) -> tuple[Dict[str, Any], Dict[str, str]]:
    """Return (prefixed_components, ref_map) where ref_map maps old ref tails to new."""
    prefixed: Dict[str, Any] = {}
    ref_map: Dict[str, str] = {}
    for section_key, section_dict in components.items():
        if not isinstance(section_dict, dict):
            continue
        prefixed[section_key] = {}
        for name, schema in section_dict.items():
            new_name = f"{prefix}_{name}"
            prefixed[section_key][new_name] = schema
            old_ref = f"#/components/{section_key}/{name}"
            new_ref = f"#/components/{section_key}/{new_name}"
            ref_map[old_ref] = new_ref
    return prefixed, ref_map


def merge_openapi_specs(
    main: Dict[str, Any],
    secondary: Dict[str, Any],
    *,
    secondary_prefix: str = "Massive",
) -> Dict[str, Any]:
    """Merge *secondary* spec into *main* (deep-copied). Components from
    *secondary* are prefixed with *secondary_prefix* to avoid collisions;
    ``$ref`` pointers inside the secondary paths/tags are rewritten accordingly.
    """
    merged = copy.deepcopy(main)

    main_title = main.get("info", {}).get("title", "Main")
    sec_title = secondary.get("info", {}).get("title", "Secondary")
    merged["info"] = {
        "title": "Bifrost API (merged)",
        "description": f"Merged from: {main_title}, {sec_title}",
        "version": main.get("info", {}).get("version", "0.1.0"),
    }

    sec_components = secondary.get("components") or {}
    prefixed_comps, ref_map = _prefix_components(sec_components, secondary_prefix)

    def _apply_ref_map(obj: Any) -> Any:
        if isinstance(obj, dict):
            out: Dict[str, Any] = {}
            for k, v in obj.items():
                if k == "$ref" and isinstance(v, str) and v in ref_map:
                    out[k] = ref_map[v]
                else:
                    out[k] = _apply_ref_map(v)
            return out
        if isinstance(obj, list):
            return [_apply_ref_map(item) for item in obj]
        return obj

    sec_paths = _apply_ref_map(secondary.get("paths") or {})
    sec_tags: List[Dict[str, Any]] = _apply_ref_map(secondary.get("tags") or [])

    main_paths = merged.get("paths") or {}
    for path_key, path_val in sec_paths.items():
        if path_key in main_paths:
            logger.warning("Path conflict %s — keeping main spec version", path_key)
        else:
            main_paths[path_key] = path_val
    merged["paths"] = main_paths

    existing_tag_names = {t["name"] for t in (merged.get("tags") or []) if isinstance(t, dict)}
    merged_tags = list(merged.get("tags") or [])
    for tag in sec_tags:
        if isinstance(tag, dict) and tag.get("name") not in existing_tag_names:
            merged_tags.append(tag)
    merged["tags"] = merged_tags

    merged_components = merged.get("components") or {}
    for section_key, section_dict in prefixed_comps.items():
        if section_key not in merged_components:
            merged_components[section_key] = {}
        merged_components[section_key].update(section_dict)
    merged["components"] = merged_components

    return merged


def main_cli() -> None:
    """CLI: fetch two specs and print merged JSON to stdout."""
    import argparse

    parser = argparse.ArgumentParser(description="Merge two OpenAPI specs")
    parser.add_argument(
        "--main-url",
        default=None,
        help="Main Monitor OpenAPI URL (default: from read_config server.monitor_port on 127.0.0.1).",
    )
    parser.add_argument(
        "--secondary-url",
        default=None,
        help="Optional secondary OpenAPI URL (e.g. Research). Massive domain retired.",
    )
    parser.add_argument("--prefix", default="Research")
    parser.add_argument("-o", "--output", help="Write to file instead of stdout")
    args = parser.parse_args()

    main_url = args.main_url
    secondary_url = args.secondary_url
    if main_url is None or secondary_url is None:
        from bifrost_core.config.startup import read_config

        cfg, _ = read_config()
        srv = cfg["server"]
        mp = int(srv["monitor_port"])
        research_p = int(srv["research_port"])
        if main_url is None:
            main_url = f"http://127.0.0.1:{mp}/openapi.json"
        if secondary_url is None:
            secondary_url = f"http://127.0.0.1:{research_p}/openapi.json"

    main_spec = fetch_openapi(main_url)
    sec_spec = fetch_openapi(secondary_url)
    merged = merge_openapi_specs(main_spec, sec_spec, secondary_prefix=args.prefix)

    text = json.dumps(merged, indent=2, ensure_ascii=False)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(text)


if __name__ == "__main__":
    main_cli()
