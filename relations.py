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


def generate_aggregation(field):
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

        facet_stage = {
            "$facet": {
                **{
                    case['value']: [
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
                        {"$count": "count"}
                    ]
                    for case in cases
                },
                "unknown": [
                    {"$match": {field['discriminator']: {"$nin": known_types}}},
                    {"$count": "count"}
                ]
            }
        }

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
        lookup_stage = {
            "$lookup": {
                "from": field['references_collection'],
                "localField": lookup_field,
                "foreignField": field['references_field'],
                "as": "reference_check"
            }
        }
        aggregation_pipeline.append(lookup_stage)

        match_stage = {
            "$match": {
                "reference_check": { "$size": 0 }
            }
        }

        count_stage = {"$count": "missing_references"}
        aggregation_pipeline.extend([match_stage, count_stage])

    return aggregation_pipeline


def validate_referential_integrity(db, config, target_collection=None):
    results = {}

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

            field_result = {}

            if 'discriminator' in field:
                field_result["discriminator"] = field['discriminator']
                field_result["cases"] = {}
                print(f"  Checking field: {collection_name}.{field_path} with discriminator {field['discriminator']}", file=sys.stderr)
                for case in field['cases']:
                    field_result["cases"][case['value']] = {
                        "references": f"{case['references_collection']}._id"
                    }
                    print(f"    Case: {case['value']} -> {case['references_collection']}._id", file=sys.stderr)
            else:
                field_result["references"] = f"{field['references_collection']}.{field['references_field']}"
                print(f"  Checking field: {collection_name}.{field_path} -> {field['references_collection']}.{field['references_field']}", file=sys.stderr)

            aggregation_pipeline = generate_aggregation(field)
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

    parser = argparse.ArgumentParser(description='Validate referential integrity in MongoDB')
    parser.add_argument('mongo_url', help='MongoDB connection URL')
    parser.add_argument('config_file', help='Path to YAML config file')
    parser.add_argument('collection', nargs='?', default=None, help='Optional: specific collection to check')
    parser.add_argument('--json', action='store_true', help='Output results as JSON at the end')

    args = parser.parse_args()

    config = load_config(args.config_file)
    client = get_db_connection(args.mongo_url)
    db = client.get_database()

    results = validate_referential_integrity(db, config, args.collection)

    if args.json:
        print(json.dumps(results, indent=2))
