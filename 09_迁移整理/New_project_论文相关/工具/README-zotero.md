# Zotero Local Reader

This workspace now contains a read-only Zotero CLI:

```bash
python3 tools/zotero_cli.py search "platform society"
python3 tools/zotero_cli.py search "跨境电商" --collection "毕业论文参考文献" --limit 20
python3 tools/zotero_cli.py show W5EDKSA7
python3 tools/zotero_cli.py collections --limit 50
```

Defaults:

- Reads the local Zotero database from `~/Zotero/zotero.sqlite`
- Resolves imported attachments under `~/Zotero/storage`
- Opens the live database in read-only mode and copies it to an in-memory snapshot before querying

Optional:

```bash
ZOTERO_DATA_DIR=/path/to/Zotero python3 tools/zotero_cli.py search "keyword" --json
```

Useful for writing:

- Search by title, abstract, author, DOI, URL, or tags
- Filter results by collection name
- Inspect one item with metadata, collections, notes, and attachment paths
- List collections with hierarchy and item counts
