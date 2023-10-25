import toml
import strawberry

from typing import Optional, List
from strawberry.schema.config import StrawberryConfig
from sdrdm_database.graphql import prepare_graphql

# Prepare a single query
env = toml.load(open("env.toml"))
model, resolver_fun = prepare_graphql(table="Root", **env)


@strawberry.type
class Query:
    @strawberry.field
    def roots(self, id: Optional[str] = None) -> List[model]:
        return resolver_fun(id=id)


schema = strawberry.Schema(
    query=Query,
    config=StrawberryConfig(auto_camel_case=False),
)
