def relation_from_name(adapter, name: str, **kwargs):
    """reverse-engineer a relation from a given name and
    the adapter. The relation name is split by the '.' character.
    """

    # Different adapters have different Relation classes
    cls = adapter.Relation
    credentials = adapter.config.credentials
    quote_policy = cls.get_default_quote_policy().to_dict()
    include_policy = cls.get_default_include_policy().to_dict()

    # Make sure we have database/schema/identifier parts, even if
    # only identifier was supplied.
    relation_parts = name.split(".")
    if len(relation_parts) == 1:
        relation_parts.insert(0, credentials.schema)
    if len(relation_parts) == 2:
        relation_parts.insert(0, credentials.database)

    _kwargs = {
        "database": relation_parts[0],
        "schema": relation_parts[1],
        "identifier": relation_parts[2],
    }
    _kwargs.update(kwargs)

    relation = cls.create(
        include_policy=include_policy,
        quote_policy=quote_policy,
        **_kwargs,
    )
    return relation
