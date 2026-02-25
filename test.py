s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2001/20260219-9ab2f8bf-c82a-45ed-9548-7098f081703d-1-1.xml.gz

Develop a PySpark job for the nonvee-uiq-info pipeline in EMR Workspace using a new design approach.

The job will read interval XML data from S3, process and transform it as per the existing pipeline logic, and load the data into target tables (usage_nonvee.reading_ivl_nonvee_{opco} and xfrm_interval.reading_ivl_nonvee_incr).

The implementation will support incremental processing based on timestamp logic and will initially be developed and tested for the OH opco.



s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/
s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2001/




PySpark job is developed in EMR Workspace for the nonvee-uiq-info pipeline.

The job successfully reads .xml.gz files from the specified S3 source location.

Incremental logic works correctly using the last processed timestamp file.

Only new/modified data between last_processed_ts and current_ts is processed.

Required transformations (as per the existing pipeline logic) are correctly implemented.

Data is successfully:

Merged into usage_nonvee.reading_ivl_nonvee_{opco}

Appended into xfrm_interval.reading_ivl_nonvee_incr

Data older than 8 days is deleted from xfrm_interval.reading_ivl_nonvee_incr.

Job is tested successfully for OH opco with sample files.

No duplicate records are created after re-running the job.

Code includes proper reference comments to previous scripts/DDL logic.
