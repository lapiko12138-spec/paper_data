#!/usr/bin/env python3
"""Read-only Zotero CLI for local literature search."""

from __future__ import annotations

import argparse
import codecs
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_DATA_DIR = Path(os.environ.get("ZOTERO_DATA_DIR", "~/Zotero")).expanduser()


class ZoteroLibrary:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.db_path = data_dir / "zotero.sqlite"
        if not self.db_path.exists():
            raise FileNotFoundError(f"Zotero database not found: {self.db_path}")
        self.conn = self._open_snapshot()
        self.conn.row_factory = sqlite3.Row

    def _open_snapshot(self) -> sqlite3.Connection:
        source = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            snapshot = sqlite3.connect(":memory:")
            source.backup(snapshot)
        finally:
            source.close()
        return snapshot

    def close(self) -> None:
        self.conn.close()

    def search_items(
        self,
        query: str,
        limit: int,
        collection: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        like_query = f"%{query.strip().lower()}%"
        params: List[Any] = [like_query]
        collection_clause = ""
        if collection:
            collection_clause = """
            AND EXISTS (
                SELECT 1
                FROM collectionItems ci
                JOIN collections c ON c.collectionID = ci.collectionID
                WHERE ci.itemID = i.itemID
                  AND lower(c.collectionName) LIKE ?
            )
            """
            params.append(f"%{collection.strip().lower()}%")
        params.append(limit)
        rows = self.conn.execute(
            f"""
            WITH item_fields AS (
                SELECT
                    i.itemID,
                    i.key,
                    it.typeName,
                    i.dateAdded,
                    i.dateModified,
                    MAX(CASE WHEN f.fieldName = 'title' THEN idv.value END) AS title,
                    MAX(CASE WHEN f.fieldName = 'abstractNote' THEN idv.value END) AS abstract_note,
                    MAX(CASE WHEN f.fieldName = 'date' THEN idv.value END) AS item_date,
                    MAX(CASE WHEN f.fieldName = 'publicationTitle' THEN idv.value END) AS publication_title,
                    MAX(CASE WHEN f.fieldName = 'DOI' THEN idv.value END) AS doi,
                    MAX(CASE WHEN f.fieldName = 'url' THEN idv.value END) AS url
                FROM items i
                JOIN itemTypes it ON it.itemTypeID = i.itemTypeID
                LEFT JOIN itemData id ON id.itemID = i.itemID
                LEFT JOIN fields f ON f.fieldID = id.fieldID
                LEFT JOIN itemDataValues idv ON idv.valueID = id.valueID
                WHERE it.typeName NOT IN ('attachment', 'note', 'annotation')
                GROUP BY i.itemID
            )
            SELECT *
            FROM item_fields i
            WHERE (
                lower(COALESCE(i.title, '')) LIKE ?
                OR lower(COALESCE(i.abstract_note, '')) LIKE ?
                OR lower(COALESCE(i.publication_title, '')) LIKE ?
                OR lower(COALESCE(i.doi, '')) LIKE ?
                OR lower(COALESCE(i.url, '')) LIKE ?
                OR EXISTS (
                    SELECT 1
                    FROM itemCreators ic
                    JOIN creators c ON c.creatorID = ic.creatorID
                    WHERE ic.itemID = i.itemID
                      AND lower(trim(COALESCE(c.firstName, '') || ' ' || COALESCE(c.lastName, ''))) LIKE ?
                )
                OR EXISTS (
                    SELECT 1
                    FROM itemTags it2
                    JOIN tags t ON t.tagID = it2.tagID
                    WHERE it2.itemID = i.itemID
                      AND lower(t.name) LIKE ?
                )
            )
            {collection_clause}
            ORDER BY COALESCE(i.item_date, ''), COALESCE(i.title, '')
            LIMIT ?
            """,
            [like_query, like_query, like_query, like_query, like_query, like_query, like_query, *params[1:]],
        ).fetchall()
        return [self._summary_from_row(row) for row in rows]

    def get_item(self, item_ref: str) -> Dict[str, Any]:
        row = self.conn.execute(
            """
            WITH item_fields AS (
                SELECT
                    i.itemID,
                    i.key,
                    it.typeName,
                    i.libraryID,
                    i.dateAdded,
                    i.dateModified,
                    MAX(CASE WHEN f.fieldName = 'title' THEN idv.value END) AS title,
                    MAX(CASE WHEN f.fieldName = 'abstractNote' THEN idv.value END) AS abstract_note,
                    MAX(CASE WHEN f.fieldName = 'date' THEN idv.value END) AS item_date,
                    MAX(CASE WHEN f.fieldName = 'publicationTitle' THEN idv.value END) AS publication_title,
                    MAX(CASE WHEN f.fieldName = 'DOI' THEN idv.value END) AS doi,
                    MAX(CASE WHEN f.fieldName = 'url' THEN idv.value END) AS url,
                    MAX(CASE WHEN f.fieldName = 'shortTitle' THEN idv.value END) AS short_title
                FROM items i
                JOIN itemTypes it ON it.itemTypeID = i.itemTypeID
                LEFT JOIN itemData id ON id.itemID = i.itemID
                LEFT JOIN fields f ON f.fieldID = id.fieldID
                LEFT JOIN itemDataValues idv ON idv.valueID = id.valueID
                WHERE i.itemID = ? OR i.key = ?
                GROUP BY i.itemID
            )
            SELECT * FROM item_fields
            """,
            [self._maybe_int(item_ref), item_ref],
        ).fetchone()
        if row is None:
            raise KeyError(f"Item not found: {item_ref}")

        data = dict(row)
        data["type"] = data.pop("typeName", None)
        data["abstractNote"] = normalize_text(data.pop("abstract_note", None))
        data["publicationTitle"] = normalize_text(data.pop("publication_title", None))
        data["shortTitle"] = normalize_text(data.pop("short_title", None))
        data["title"] = normalize_text(data.get("title"))
        data["doi"] = normalize_text(data.get("doi"))
        data["url"] = normalize_text(data.get("url"))
        data["creators"] = self._get_creators(row["itemID"])
        data["tags"] = self._get_tags(row["itemID"])
        data["collections"] = self._get_collections(row["itemID"])
        data["attachments"] = self._get_attachments(row["itemID"])
        data["notes"] = self._get_notes(row["itemID"])
        data["year"] = extract_year(data.get("item_date"))
        return data

    def list_collections(self, limit: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT c.collectionID, c.collectionName, c.parentCollectionID, c.key
            FROM collections c
            ORDER BY c.collectionName
            """
        ).fetchall()
        counts = {
            row["collectionID"]: row["item_count"]
            for row in self.conn.execute(
                """
                SELECT ci.collectionID, COUNT(*) AS item_count
                FROM collectionItems ci
                JOIN items i ON i.itemID = ci.itemID
                JOIN itemTypes it ON it.itemTypeID = i.itemTypeID
                WHERE it.typeName NOT IN ('attachment', 'note', 'annotation')
                GROUP BY ci.collectionID
                """
            ).fetchall()
        }
        nodes = [dict(row) for row in rows]
        indexed = {node["collectionID"]: node for node in nodes}
        for node in nodes:
            node["item_count"] = counts.get(node["collectionID"], 0)
            node["path"] = self._collection_path(node, indexed)
        nodes.sort(key=lambda item: item["path"].lower())
        return nodes[:limit]

    def _collection_path(
        self,
        node: Dict[str, Any],
        indexed: Dict[int, Dict[str, Any]],
    ) -> str:
        parts = [node["collectionName"]]
        parent_id = node["parentCollectionID"]
        while parent_id:
            parent = indexed.get(parent_id)
            if not parent:
                break
            parts.append(parent["collectionName"])
            parent_id = parent["parentCollectionID"]
        return " / ".join(reversed(parts))

    def _summary_from_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        item_id = row["itemID"]
        return {
            "itemID": item_id,
            "key": row["key"],
            "type": row["typeName"],
            "title": normalize_text(row["title"]),
            "year": extract_year(row["item_date"]),
            "date": row["item_date"],
            "publicationTitle": normalize_text(row["publication_title"]),
            "doi": normalize_text(row["doi"]),
            "url": normalize_text(row["url"]),
            "creators": self._get_creators(item_id),
            "tags": self._get_tags(item_id),
            "collections": self._get_collections(item_id),
            "attachments": self._get_attachments(item_id),
        }

    def _get_creators(self, item_id: int) -> List[Dict[str, str]]:
        rows = self.conn.execute(
            """
            SELECT
                ic.orderIndex,
                ct.creatorType AS creator_type,
                c.firstName,
                c.lastName
            FROM itemCreators ic
            JOIN creators c ON c.creatorID = ic.creatorID
            JOIN creatorTypes ct ON ct.creatorTypeID = ic.creatorTypeID
            WHERE ic.itemID = ?
            ORDER BY ic.orderIndex
            """,
            [item_id],
        ).fetchall()
        creators = []
        for row in rows:
            first_name = normalize_text(row["firstName"])
            last_name = normalize_text(row["lastName"])
            display = " ".join(part for part in [first_name, last_name] if part).strip()
            creators.append(
                {
                    "type": row["creator_type"],
                    "name": display or (last_name or first_name or ""),
                }
            )
        return creators

    def _get_tags(self, item_id: int) -> List[str]:
        rows = self.conn.execute(
            """
            SELECT t.name
            FROM itemTags it
            JOIN tags t ON t.tagID = it.tagID
            WHERE it.itemID = ?
            ORDER BY t.name
            """,
            [item_id],
        ).fetchall()
        return [normalize_text(row["name"]) for row in rows]

    def _get_collections(self, item_id: int) -> List[str]:
        rows = self.conn.execute(
            """
            SELECT c.collectionID, c.collectionName, c.parentCollectionID
            FROM collectionItems ci
            JOIN collections c ON c.collectionID = ci.collectionID
            WHERE ci.itemID = ?
            ORDER BY c.collectionName
            """,
            [item_id],
        ).fetchall()
        indexed = {
            row["collectionID"]: {
                "collectionID": row["collectionID"],
                "collectionName": row["collectionName"],
                "parentCollectionID": row["parentCollectionID"],
            }
            for row in self.conn.execute(
                "SELECT collectionID, collectionName, parentCollectionID FROM collections"
            ).fetchall()
        }
        return [normalize_text(self._collection_path(dict(row), indexed)) for row in rows]

    def _get_attachments(self, item_id: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
                child.itemID,
                child.key,
                ia.parentItemID,
                ia.linkMode,
                ia.contentType,
                ia.path
            FROM itemAttachments ia
            JOIN items child ON child.itemID = ia.itemID
            WHERE ia.parentItemID = ?
            ORDER BY child.itemID
            """,
            [item_id],
        ).fetchall()
        attachments = []
        for row in rows:
            attachments.append(
                {
                    "itemID": row["itemID"],
                    "key": row["key"],
                    "contentType": normalize_text(row["contentType"]),
                    "linkMode": row["linkMode"],
                    "zoteroPath": normalize_text(row["path"]),
                    "localPath": self._resolve_attachment_path(row["key"], row["path"]),
                }
            )
        return attachments

    def _get_notes(self, item_id: int) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT itemID, title, note
            FROM itemNotes
            WHERE parentItemID = ?
            ORDER BY itemID
            """,
            [item_id],
        ).fetchall()
        return [
            {
                "itemID": row["itemID"],
                "title": normalize_text(row["title"]),
                "notePreview": compact_text(normalize_text(row["note"])),
            }
            for row in rows
        ]

    def _resolve_attachment_path(self, item_key: str, zotero_path: Optional[str]) -> Optional[str]:
        if not zotero_path:
            return None
        if zotero_path.startswith("storage:"):
            filename = zotero_path.split("storage:", 1)[1]
            return str(self.data_dir / "storage" / item_key / filename)
        if zotero_path.startswith("attachments:"):
            filename = zotero_path.split("attachments:", 1)[1]
            return str(self.data_dir / filename)
        if os.path.isabs(zotero_path):
            return zotero_path
        return zotero_path

    @staticmethod
    def _maybe_int(value: str) -> Any:
        try:
            return int(value)
        except ValueError:
            return value


def extract_year(raw_date: Optional[str]) -> Optional[str]:
    if not raw_date:
        return None
    for token in raw_date.split():
        if len(token) >= 4 and token[:4].isdigit():
            return token[:4]
    return raw_date[:4] if len(raw_date) >= 4 else raw_date


def compact_text(text: Optional[str], limit: int = 180) -> str:
    if not text:
        return ""
    collapsed = " ".join(text.replace("\n", " ").replace("\r", " ").split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


def normalize_text(text: Optional[str]) -> Optional[str]:
    if text is None or "\\u" not in text:
        return text
    try:
        return codecs.decode(text, "unicode_escape")
    except Exception:
        return text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read local Zotero metadata.")
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="Zotero data directory (default: %(default)s)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Search literature")
    search_parser.add_argument("query", help="Keyword query")
    search_parser.add_argument("--limit", type=int, default=10, help="Maximum number of results")
    search_parser.add_argument(
        "--collection",
        help="Filter by collection name (partial match)",
    )
    search_parser.add_argument("--json", action="store_true", help="Emit JSON output")

    show_parser = subparsers.add_parser("show", help="Show one item")
    show_parser.add_argument("item", help="Zotero item key or numeric itemID")
    show_parser.add_argument("--json", action="store_true", help="Emit JSON output")

    collections_parser = subparsers.add_parser("collections", help="List collections")
    collections_parser.add_argument("--limit", type=int, default=100, help="Maximum number of rows")
    collections_parser.add_argument("--json", action="store_true", help="Emit JSON output")

    return parser


def render_search_text(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "No matching items."
    lines: List[str] = []
    for item in items:
        creators = ", ".join(creator["name"] for creator in item["creators"][:4]) or "Unknown author"
        year = item["year"] or "n.d."
        publication = item.get("publicationTitle") or ""
        collections = "; ".join(item["collections"][:2])
        attachments = ", ".join(
            Path(att["localPath"]).name for att in item["attachments"] if att.get("localPath")
        )
        lines.append(f"[{item['key']}] {item.get('title') or '(untitled)'}")
        lines.append(f"  {creators} | {year} | {item['type']}")
        if publication:
            lines.append(f"  Source: {publication}")
        if collections:
            lines.append(f"  Collections: {collections}")
        if attachments:
            lines.append(f"  Files: {attachments}")
    return "\n".join(lines)


def render_show_text(item: Dict[str, Any]) -> str:
    lines = [
        f"[{item['key']}] {item.get('title') or '(untitled)'}",
        f"Type: {item.get('type')} | Year: {item.get('year') or 'n.d.'}",
    ]
    creators = ", ".join(f"{creator['name']} ({creator['type']})" for creator in item["creators"])
    if creators:
        lines.append(f"Creators: {creators}")
    if item.get("publicationTitle"):
        lines.append(f"Source: {item['publicationTitle']}")
    if item.get("doi"):
        lines.append(f"DOI: {item['doi']}")
    if item.get("url"):
        lines.append(f"URL: {item['url']}")
    if item.get("abstractNote"):
        lines.append(f"Abstract: {compact_text(item['abstractNote'], 500)}")
    if item["tags"]:
        lines.append(f"Tags: {', '.join(item['tags'])}")
    if item["collections"]:
        lines.append(f"Collections: {'; '.join(item['collections'])}")
    if item["attachments"]:
        lines.append("Attachments:")
        for attachment in item["attachments"]:
            lines.append(
                f"  - [{attachment['key']}] {attachment.get('contentType') or 'unknown'} | "
                f"{attachment.get('localPath') or attachment.get('zoteroPath') or ''}"
            )
    if item["notes"]:
        lines.append("Notes:")
        for note in item["notes"]:
            lines.append(f"  - [{note['itemID']}] {note['notePreview']}")
    return "\n".join(lines)


def render_collections_text(collections: List[Dict[str, Any]]) -> str:
    if not collections:
        return "No collections found."
    return "\n".join(
        f"[{item['key']}] {item['path']} ({item['item_count']} items)" for item in collections
    )


def main() -> int:
    args = build_parser().parse_args()
    library = ZoteroLibrary(Path(args.data_dir).expanduser())
    try:
        if args.command == "search":
            result = library.search_items(args.query, args.limit, args.collection)
        elif args.command == "show":
            result = library.get_item(args.item)
        else:
            result = library.list_collections(args.limit)
    finally:
        library.close()

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "search":
        print(render_search_text(result))
    elif args.command == "show":
        print(render_show_text(result))
    else:
        print(render_collections_text(result))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, KeyError, sqlite3.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
