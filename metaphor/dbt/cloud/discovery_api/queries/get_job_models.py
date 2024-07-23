from typing import List, Optional

from gql import Client, gql
from pydantic import BaseModel, Field

from metaphor.common.entity_id import dataset_normalized_name

_query = gql(
    """
    query Models($jobId: BigInt!) {
        job(id: $jobId) {
            models {
                alias
                database
                name
                schema
                uniqueId
            }
        }
    }
    """
)


class Model(BaseModel):
    alias: Optional[str]
    database: Optional[str]
    name: str
    uniqueId: str
    schema_: Optional[str] = Field(alias="schema")

    @property
    def normalized_name(self) -> str:
        return dataset_normalized_name(self.database, self.schema_, self.name)


class Job(BaseModel):
    models: List[Model]


class GetJobModels(BaseModel):
    job: Job


def get_job_models(client: Client, job_id: int) -> GetJobModels:
    res = client.execute(_query, variable_values={"jobId": job_id})
    return GetJobModels.model_validate(res)
