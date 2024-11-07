class Queries:
    """
    Utility class for all queries used by the BQ crawler.

    See https://cloud.google.com/logging/docs/view/logging-query-language for query syntax.
    """

    @staticmethod
    def log_query_filter(
        service_account_filter: str,
        destination_table_filter: str,
        start_time: str,
        end_time: str,
    ):
        """
        Returns the filter part of a log query.
        """
        return f"""
            resource.type="bigquery_project" AND
            protoPayload.serviceName="bigquery.googleapis.com" AND
            protoPayload.metadata.jobChange.after="DONE" AND
            NOT protoPayload.metadata.jobChange.job.jobStatus.errorResult.code:* AND
            protoPayload.metadata.jobChange.job.jobConfig.type=("COPY" OR "QUERY") AND
            {service_account_filter}
            {destination_table_filter}
            timestamp>="{start_time}" AND
            timestamp<"{end_time}"
        """

    @staticmethod
    def dll(db: str, schema: str) -> str:
        return f"select table_name, ddl from `{db}.{schema}.INFORMATION_SCHEMA.TABLES`"
