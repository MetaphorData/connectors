from datetime import datetime
from typing import List, Optional

from gql import Client, gql
from pydantic import BaseModel

_query = gql(
    """
    query Tests($jobId: BigInt!) {
        job(id: $jobId) {
            tests {
                uniqueId
                name
                status
                columnName
                executeCompletedAt
                dependsOn
            }
        }
    }
    """
)


class Test(BaseModel):
    uniqueId: str
    name: Optional[str]
    status: Optional[str]
    columnName: Optional[str]
    executeCompletedAt: Optional[datetime]
    dependsOn: List[str]

    @property
    def models(self) -> List[str]:
        return [x for x in self.dependsOn if x.startswith("model.")]


class Job(BaseModel):
    tests: List[Test]


class GetJobTests(BaseModel):
    job: Job


def get_job_tests(client: Client, job_id: int) -> GetJobTests:
    res = client.execute(_query, variable_values={"jobId": job_id})
    return GetJobTests.model_validate(res)
