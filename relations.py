import json
import yaml
import sys
from pymongo import MongoClient


def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def get_db_connection(mongo_url):
    client = MongoClient(mongo_url)
    return client


def generate_find_orphans_query(collection_name, field):
    """Generate an aggregation pipeline to find orphaned document IDs (not just count)."""
    pipeline = []

    lookup_field = field['field']
    if 'sub_field' in field:
        lookup_field = f"{lookup_field}.{field['sub_field']}"

    if field.get('is_array', False):
        pipeline.append({"$unwind": f"${field['field']}"})

    if field.get('optional', False):
        pipeline.append({"$match": {lookup_field: {"$ne": None}}})

    if field.get('is_stupid_string', False):
        safe_lookup_name = lookup_field.replace('.', '_')
        converted_field = f"_converted_{safe_lookup_name}"
        pipeline.append({
            "$addFields": {
                converted_field: {
                    "$cond": {
                        "if": {"$and": [
                            {"$ne": [f"${lookup_field}", None]},
                            {"$eq": [{"$type": f"${lookup_field}"}, "string"]}
                        ]},
                        "then": {"$toObjectId": f"${lookup_field}"},
                        "else": f"${lookup_field}"
                    }
                }
            }
        })
        lookup_field = converted_field

    if 'discriminator' in field:
        # For discriminator fields, generate separate queries per case
        queries = {}
        for case in field['cases']:
            case_pipeline = pipeline.copy()
            case_pipeline.extend([
                {"$match": {field['discriminator']: case['value']}},
                {
                    "$lookup": {
                        "from": case['references_collection'],
                        "localField": lookup_field,
                        "foreignField": "_id",
                        "as": "reference_check"
                    }
                },
                {"$match": {"reference_check": {"$size": 0}}},
                {"$project": {"_id": 1}}
            ])
            queries[case['value']] = case_pipeline
        return queries
    else:
        pipeline.extend([
            {
                "$lookup": {
                    "from": field['references_collection'],
                    "localField": lookup_field,
                    "foreignField": field['references_field'],
                    "as": "reference_check"
                }
            },
            {"$match": {"reference_check": {"$size": 0}}},
            {"$project": {"_id": 1}}
        ])
        return pipeline


def generate_cleanup_query(collection_name, field):
    """Generate a cleanup query/strategy for orphaned documents."""
    cleanup = {}

    lookup_field = field['field']
    if 'sub_field' in field:
        lookup_field = f"{lookup_field}.{field['sub_field']}"

    if 'discriminator' in field:
        cleanup['strategy'] = 'per_case'
        cleanup['cases'] = {}
        for case in field['cases']:
            # For each case, we can delete documents or unset the field
            cleanup['cases'][case['value']] = {
                'delete': f'db["{collection_name}"].deleteMany({{ _id: {{ $in: <orphan_ids> }} }})',
                'unset_field': f'db["{collection_name}"].updateMany({{ _id: {{ $in: <orphan_ids> }} }}, {{ $unset: {{ "{lookup_field}": "" }} }})',
                'note': f"Replace <orphan_ids> with IDs from the find query for case '{case['value']}'"
            }
    else:
        cleanup['strategy'] = 'single'
        cleanup['delete'] = f'db["{collection_name}"].deleteMany({{ _id: {{ $in: <orphan_ids> }} }})'
        cleanup['unset_field'] = f'db["{collection_name}"].updateMany({{ _id: {{ $in: <orphan_ids> }} }}, {{ $unset: {{ "{lookup_field}": "" }} }})'
        cleanup['note'] = "Replace <orphan_ids> with IDs from the find query"

    return cleanup


def print_queries_for_collection(config, collection_name, target_field=None):
    """Print the find and cleanup queries for a specific collection and optionally a specific field."""
    for relation in config['relations']:
        if relation['collection'] != collection_name:
            continue

        print(f"\n{'='*60}")
        if target_field:
            print(f"QUERIES FOR: {collection_name}.{target_field}")
        else:
            print(f"QUERIES FOR COLLECTION: {collection_name}")
        print(f"{'='*60}")

        found_field = False
        for field in relation['fields']:
            field_path = field['field']
            if 'sub_field' in field:
                field_path = f"{field_path}.{field['sub_field']}"

            # Skip if a specific field was requested and this isn't it
            if target_field and field_path != target_field:
                continue

            found_field = True
            print(f"\n--- Field: {field_path} ---")

            # Show preload_ids optimization info
            if field.get('preload_ids', False):
                print(f"\n  [OPTIMIZATION: preload_ids=true]")
                print(f"  At runtime, IDs are preloaded and $nin is used instead of $lookup.")
                print(f"  The query below shows the $lookup version for manual use.")

            # Find orphans query
            find_query = generate_find_orphans_query(collection_name, field)

            if 'discriminator' in field:
                print(f"\nDiscriminator: {field['discriminator']}")
                for case_value, case_pipeline in find_query.items():
                    print(f"\n  Case '{case_value}':")
                    print(f"  Find orphaned documents:")
                    print(f'    db["{collection_name}"].aggregate(')
                    print(f"      {json.dumps(case_pipeline, indent=6)}")
                    print(f"    )")
            else:
                print(f"\nFind orphaned documents:")
                print(f'  db["{collection_name}"].aggregate(')
                print(f"    {json.dumps(find_query, indent=4)}")
                print(f"  )")

            # Cleanup queries
            cleanup = generate_cleanup_query(collection_name, field)

            print(f"\nCleanup options:")
            if cleanup['strategy'] == 'per_case':
                for case_value, case_cleanup in cleanup['cases'].items():
                    print(f"\n  Case '{case_value}':")
                    print(f"    Delete documents:  {case_cleanup['delete']}")
                    print(f"    Or unset field:    {case_cleanup['unset_field']}")
            else:
                print(f"  Delete documents:  {cleanup['delete']}")
                print(f"  Or unset field:    {cleanup['unset_field']}")
                print(f"  Note: {cleanup['note']}")

        if target_field and not found_field:
            print(f"\nField '{target_field}' not found in collection '{collection_name}'.")
            return False

        return True

    print(f"Collection '{collection_name}' not found in config.")
    return False


def generate_aggregation(field, preloaded_ids=None):
    """
    Generate aggregation pipeline for finding orphaned references.

    If preloaded_ids is provided (dict mapping collection -> set of IDs),
    uses fast $nin matching instead of slow $lookup joins.
    """
    aggregation_pipeline = []

    if field.get('is_array', False):
        aggregation_pipeline.append({"$unwind": f"${field['field']}"})

    # Determine the actual field to look up
    lookup_field = field['field']
    if 'sub_field' in field:
        lookup_field = f"{lookup_field}.{field['sub_field']}"

    if field.get('optional', False):
        aggregation_pipeline.append({"$match": {lookup_field: {"$ne": None}}})

    # Handle is_string: convert string field to ObjectId for lookup
    if field.get('is_stupid_string', False):
        # Use a safe field name for conversion, avoiding dots in the new field name
        safe_lookup_name = lookup_field.replace('.', '_')
        converted_field = f"_converted_{safe_lookup_name}"
        aggregation_pipeline.append({
            "$addFields": {
                converted_field: {
                    "$cond": {
                        "if": {"$and": [
                            {"$ne": [f"${lookup_field}", None]},
                            {"$eq": [{"$type": f"${lookup_field}"}, "string"]}
                        ]},
                        "then": {"$toObjectId": f"${lookup_field}"},
                        "else": f"${lookup_field}"
                    }
                }
            }
        })
        lookup_field = converted_field

    # Handle cases where there's a discriminator (e.g., boardElementType)
    if 'discriminator' in field:
        cases = field['cases']
        known_types = [case['value'] for case in cases]

        facet_branches = {}
        for case in cases:
            ref_collection = case['references_collection']

            # Check if we have preloaded IDs for this collection
            if preloaded_ids and ref_collection in preloaded_ids:
                # Fast path: use $nin with preloaded IDs
                facet_branches[case['value']] = [
                    {"$match": {field['discriminator']: case['value']}},
                    {"$match": {lookup_field: {"$nin": list(preloaded_ids[ref_collection])}}},
                    {"$count": "count"}
                ]
            else:
                # Slow path: use $lookup
                facet_branches[case['value']] = [
                    {"$match": {field['discriminator']: case['value']}},
                    {
                        "$lookup": {
                            "from": ref_collection,
                            "localField": lookup_field,
                            "foreignField": "_id",
                            "as": "reference_check"
                        }
                    },
                    {"$match": {"reference_check": {"$size": 0}}},
                    {"$count": "count"}
                ]

        facet_branches["unknown"] = [
            {"$match": {field['discriminator']: {"$nin": known_types}}},
            {"$count": "count"}
        ]

        facet_stage = {"$facet": facet_branches}
        aggregation_pipeline.append(facet_stage)

        project_stage = {
            "$project": {
                **{
                    f"count_{case['value']}": {"$ifNull": [{"$arrayElemAt": [f"${case['value']}.count", 0]}, 0]}
                    for case in cases
                },
                "count_unknown": {"$ifNull": [{"$arrayElemAt": ["$unknown.count", 0]}, 0]},
                "missing_references": {
                    "$add": [
                        *[
                            {"$ifNull": [{"$arrayElemAt": [f"${case['value']}.count", 0]}, 0]}
                            for case in cases
                        ],
                        {"$ifNull": [{"$arrayElemAt": ["$unknown.count", 0]}, 0]}
                    ]
                }
            }
        }

        aggregation_pipeline.append(project_stage)

    else:
        ref_collection = field['references_collection']
        ref_field = field['references_field']

        # Check if we have preloaded IDs for this collection
        if preloaded_ids and ref_collection in preloaded_ids:
            # Fast path: use $nin with preloaded IDs
            aggregation_pipeline.append({
                "$match": {lookup_field: {"$nin": list(preloaded_ids[ref_collection])}}
            })
        else:
            # Slow path: use $lookup
            lookup_stage = {
                "$lookup": {
                    "from": ref_collection,
                    "localField": lookup_field,
                    "foreignField": ref_field,
                    "as": "reference_check"
                }
            }
            aggregation_pipeline.append(lookup_stage)

            match_stage = {
                "$match": {
                    "reference_check": { "$size": 0 }
                }
            }
            aggregation_pipeline.append(match_stage)

        count_stage = {"$count": "missing_references"}
        aggregation_pipeline.append(count_stage)

    return aggregation_pipeline


def validate_referential_integrity(db, config, target_collection=None, target_field=None):
    results = {}

    # First pass: identify collections that need ID preloading
    collections_to_preload = set()
    for relation in config['relations']:
        if target_collection and relation['collection'] != target_collection:
            continue
        for field in relation['fields']:
            # Get field path for filtering
            field_path = field['field']
            if 'sub_field' in field:
                field_path = f"{field_path}.{field['sub_field']}"

            # Skip if targeting a specific field and this isn't it
            if target_field and field_path != target_field:
                continue

            if field.get('preload_ids', False):
                if 'discriminator' in field:
                    for case in field['cases']:
                        collections_to_preload.add(case['references_collection'])
                else:
                    collections_to_preload.add(field['references_collection'])

    # Preload IDs from small collections
    preloaded_ids = {}
    if collections_to_preload:
        print(f"Preloading IDs from {len(collections_to_preload)} collection(s)...", file=sys.stderr)
        for coll_name in collections_to_preload:
            print(f"  Loading IDs from '{coll_name}'...", file=sys.stderr)
            ids = set(doc['_id'] for doc in db[coll_name].find({}, {'_id': 1}))
            preloaded_ids[coll_name] = ids
            print(f"    Loaded {len(ids)} IDs", file=sys.stderr)

    for relation in config['relations']:
        collection_name = relation['collection']

        if target_collection and collection_name != target_collection:
            continue

        collection = db[collection_name]
        print(f"Processing collection: {collection_name}", file=sys.stderr)

        if collection_name not in results:
            results[collection_name] = {"fields": {}}

        for field in relation['fields']:
            field_path = field['field']
            if 'sub_field' in field:
                field_path = f"{field_path}.{field['sub_field']}"

            # Skip if targeting a specific field and this isn't it
            if target_field and field_path != target_field:
                continue

            field_result = {}

            # Show if using fast preload mode
            using_preload = field.get('preload_ids', False) and preloaded_ids

            if 'discriminator' in field:
                field_result["discriminator"] = field['discriminator']
                field_result["cases"] = {}
                mode_info = " (preloaded)" if using_preload else ""
                print(f"  Checking field: {collection_name}.{field_path} with discriminator {field['discriminator']}{mode_info}", file=sys.stderr)
                for case in field['cases']:
                    field_result["cases"][case['value']] = {
                        "references": f"{case['references_collection']}._id"
                    }
                    print(f"    Case: {case['value']} -> {case['references_collection']}._id", file=sys.stderr)
            else:
                field_result["references"] = f"{field['references_collection']}.{field['references_field']}"
                mode_info = " (preloaded)" if using_preload else ""
                print(f"  Checking field: {collection_name}.{field_path} -> {field['references_collection']}.{field['references_field']}{mode_info}", file=sys.stderr)

            # Pass preloaded IDs if this field uses preloading
            ids_for_field = preloaded_ids if field.get('preload_ids', False) else None
            aggregation_pipeline = generate_aggregation(field, ids_for_field)
            result = list(collection.aggregate(aggregation_pipeline))

            if result:
                count = result[0].get('missing_references', 0)
                if 'discriminator' in field:
                    for case in field['cases']:
                        case_count = result[0].get(f"count_{case['value']}", 0)
                        field_result["cases"][case['value']]["missing_count"] = case_count
                        print(f"      {case['value']}: {case_count}", file=sys.stderr)
                    unknown_count = result[0].get('count_unknown', 0)
                    field_result["unknown_count"] = unknown_count
                    if unknown_count > 0:
                        print(f"      unknown: {unknown_count}", file=sys.stderr)
                field_result["total_missing"] = count
                print(f"    Found {count} dereferenced documents in field '{field_path}' of collection '{collection_name}'", file=sys.stderr)
            else:
                if 'discriminator' in field:
                    for case in field['cases']:
                        field_result["cases"][case['value']]["missing_count"] = 0
                    field_result["unknown_count"] = 0
                field_result["total_missing"] = 0
                print(f"    No dereferenced documents found in field '{field_path}' of collection '{collection_name}'.", file=sys.stderr)

            results[collection_name]["fields"][field_path] = field_result

    return results

if __name__ == "__main__":
    import argparse

    def parse_collection_field(target, config):
        """Parse 'collection' or 'collection.field' into (collection_name, field_name or None)."""
        # Get all collection names from config
        collection_names = [r['collection'] for r in config['relations']]

        # First, check if the entire target is a collection name
        if target in collection_names:
            return target, None

        # Try to find a collection name that the target starts with
        for coll in sorted(collection_names, key=len, reverse=True):  # Try longest first
            if target.startswith(coll + '.'):
                field = target[len(coll) + 1:]  # +1 for the dot
                return coll, field

        # If no match, assume it's just a collection name (will error later if not found)
        return target, None

    parser = argparse.ArgumentParser(
        description='Validate referential integrity in MongoDB',
        epilog='''Examples:
  python relations.py mongodb://... config.yaml                       # Check all collections
  python relations.py mongodb://... config.yaml submissions           # Check one collection  
  python relations.py mongodb://... config.yaml submissions.schoolId  # Check one specific field
  python relations.py --show-queries config.yaml submissions          # Show queries for collection
  python relations.py --show-queries config.yaml submissions.schoolId # Show queries for specific field'''
    )
    parser.add_argument('args', nargs='*', help='mongo_url config_file [collection[.field]] OR config_file collection[.field] (with --show-queries)')
    parser.add_argument('--json', action='store_true', help='Output results as JSON at the end')
    parser.add_argument('--show-queries', action='store_true', help='Show aggregation and cleanup queries instead of running them (requires collection)')

    args = parser.parse_args()

    if args.show_queries:
        # --show-queries mode: expects config_file and collection[.field]
        if len(args.args) != 2:
            print("Usage with --show-queries: python relations.py --show-queries <config_file> <collection[.field]>", file=sys.stderr)
            print("  Examples:", file=sys.stderr)
            print("    python relations.py --show-queries config.yaml submissions", file=sys.stderr)
            print("    python relations.py --show-queries config.yaml submissions.schoolId", file=sys.stderr)
            sys.exit(1)
        config_file, target = args.args
        config = load_config(config_file)
        collection, field = parse_collection_field(target, config)
        print_queries_for_collection(config, collection, field)
    else:
        # Normal mode: expects mongo_url, config_file, and optional collection[.field]
        if len(args.args) < 2 or len(args.args) > 3:
            print("Usage: python relations.py <mongo_url> <config_file> [collection[.field]]", file=sys.stderr)
            print("       python relations.py --show-queries <config_file> <collection[.field]>", file=sys.stderr)
            print("  Examples:", file=sys.stderr)
            print("    python relations.py mongodb://... config.yaml", file=sys.stderr)
            print("    python relations.py mongodb://... config.yaml submissions", file=sys.stderr)
            print("    python relations.py mongodb://... config.yaml submissions.schoolId", file=sys.stderr)
            sys.exit(1)

        mongo_url = args.args[0]
        config_file = args.args[1]

        config = load_config(config_file)

        # Parse optional collection[.field] argument
        collection = None
        field = None
        if len(args.args) == 3:
            collection, field = parse_collection_field(args.args[2], config)

        client = get_db_connection(mongo_url)
        db = client.get_database()

        results = validate_referential_integrity(db, config, collection, field)

        if args.json:
            print(json.dumps(results, indent=2))
