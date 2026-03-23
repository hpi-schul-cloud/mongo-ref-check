# mongo-ref-check

Validates referential integrity between MongoDB collections.

## Usage

```bash
python relations.py <mongo_url> <config_file> [<collection_name>]
```

## How it works

1. Define collection relationships in `relations.yaml`
2. Script runs aggregation queries to find documents with broken references (foreign keys pointing to non-existent documents)

## Config

`relations.yaml` defines:
- Collection relationships
- Simple references (`field` → `references_collection.references_field`)
- Discriminated references (polymorphic, based on a type field)
- Array fields, optional fields, string-to-ObjectId conversions

