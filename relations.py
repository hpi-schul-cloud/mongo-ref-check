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

    if field.get('optional', False):
        aggregation_pipeline.append({"$match": {field['field']: {"$ne": None}}})

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
                                "localField": field['field'],
                                "foreignField": "_id",
                                "as": "reference_check"
                            }
                        },
                        {"$match": {"reference_check": {"$size": 0}}}
                    ]
                    for case in cases
                },
                "unknown": [
                    {
                        "$match": {field['discriminator']: {"$nin": known_types}}
                    }
                ]
            }
        }

        aggregation_pipeline.append(facet_stage)

        project_stage = {
            "$project": {
                "missing_references": {
                    "$add": [
                        *[
                            {"$size": f"${case['value']}"}
                            for case in cases
                        ],
                        {"$size": "$unknown"}
                    ]
                }
            }
        }

        aggregation_pipeline.append(project_stage)

    else:
        lookup_stage = {
            "$lookup": {
                "from": field['references_collection'],
                "localField": field['field'],
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
    for relation in config['relations']:
        collection_name = relation['collection']

        if target_collection and collection_name != target_collection:
            continue

        collection = db[collection_name]
        print(f"Processing collection: {collection_name}")

        for field in relation['fields']:
            if 'discriminator' in field:
                print(
                    f"  Checking field: {collection_name}.{field['field']} with discriminator {field['discriminator']}")

                for case in field['cases']:
                    print(f"    Case: {case['value']} -> {case['references_collection']}._id")
            else:
                print(f"  Checking field: {collection_name}.{field['field']} -> {field['references_collection']}.{field['references_field']}")

            aggregation_pipeline = generate_aggregation(field)
            result = list(collection.aggregate(aggregation_pipeline))

            if result:
                count = result[0].get('missing_references', 0)
                print(
                    f"    Found {count} dereferenced documents in field '{field['field']}' of collection '{collection_name}'")
            else:
                print(
                    f"    No dereferenced documents found in field '{field['field']}' of collection '{collection_name}'.")

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python validate_references.py <mongo_url> <config_file> [<collection_name>]")
        sys.exit(1)

    mongo_url = sys.argv[1]
    config_file = sys.argv[2]
    target_collection = sys.argv[3] if len(sys.argv) == 4 else None

    config = load_config(config_file)
    client = get_db_connection(mongo_url)
    db = client.get_database()

    validate_referential_integrity(db, config, target_collection)