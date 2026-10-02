"""Transformations applied to an Ossie model before it is converted."""

import yaml

from ._io import Yaml


def model_name(ossie_yaml: str) -> str:
    name = (yaml.safe_load(ossie_yaml) or {}).get("name")
    if not name:
        raise ValueError("the Ossie model has no `name`, and no name was given")
    return name


def qualify_sources(ossie_yaml: str, prefix: str, parts: int) -> str:
    """Prefix any dataset `source:` that is not already fully qualified.

    One Ossie file should be uploadable to either platform, but the two disagree on how
    many name parts a source has - Fabric keeps schema and table, Unity Catalog wants
    catalog, schema and table. A model that names its tables bare (`source: orders`) gets
    the connection's own prefix here; one that already qualifies them is left alone.
    """
    document = yaml.safe_load(ossie_yaml)
    if not isinstance(document, dict):
        return Yaml(ossie_yaml)

    changed = False
    for dataset in document.get("datasets") or []:
        source = dataset.get("source")
        if isinstance(source, str) and source and len(source.split(".")) < parts:
            dataset["source"] = f"{prefix}.{source.split('.')[-1]}"
            changed = True
    if not changed:
        return Yaml(ossie_yaml)
    return Yaml(yaml.safe_dump(document, sort_keys=False, allow_unicode=True))


def drop_duplicate_join_keys(ossie_yaml: str) -> tuple[str, list[str]]:
    """Remove join-key fields that Unity Catalog would reject as duplicate dimensions.

    Fabric and Unity Catalog make opposite demands of the same join column. A Fabric
    relationship needs the key present as a field on *both* datasets, or the relationship
    is dropped outright. A Metric View flattens every dataset into one namespace and
    refuses two dimensions with the same name, so the same model fails to convert.

    The reconciliation is the one a person would make by hand: on the `to` side of a
    relationship - the one side - drop the key field, keeping it in `primary_key` so the
    join still resolves. The fact-side column survives, so grouping by it is unaffected.
    Returns the adjusted YAML and a description of each field removed.
    """
    document = yaml.safe_load(ossie_yaml)
    if not isinstance(document, dict):
        return Yaml(ossie_yaml), []

    datasets = {d.get("name"): d for d in document.get("datasets") or [] if isinstance(d, dict)}
    removed = []

    for relationship in document.get("relationships") or []:
        if not isinstance(relationship, dict):
            continue
        source = datasets.get(relationship.get("from"))
        target = datasets.get(relationship.get("to"))
        if not source or not target:
            continue
        source_fields = {f.get("name") for f in source.get("fields") or []}
        for from_column, to_column in zip(
            relationship.get("from_columns") or [], relationship.get("to_columns") or []
        ):
            if to_column not in source_fields and from_column not in source_fields:
                continue
            fields = target.get("fields") or []
            kept = [f for f in fields if f.get("name") != to_column]
            if len(kept) != len(fields):
                target["fields"] = kept
                if to_column not in (target.get("primary_key") or []):
                    target.setdefault("primary_key", []).append(to_column)
                removed.append(f"{target['name']}.{to_column}")

    if not removed:
        return Yaml(ossie_yaml), []
    return Yaml(yaml.safe_dump(document, sort_keys=False, allow_unicode=True)), removed
