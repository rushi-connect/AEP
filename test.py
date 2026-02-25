s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2001/20260219-9ab2f8bf-c82a-45ed-9548-7098f081703d-1-1.xml.gz

Develop a PySpark job for the nonvee-uiq-info pipeline in EMR Workspace using a new design approach.

The job will read interval XML data from S3, process and transform it as per the existing pipeline logic, and load the data into target tables (usage_nonvee.reading_ivl_nonvee_{opco} and xfrm_interval.reading_ivl_nonvee_incr).

The implementation will support incremental processing based on timestamp logic and will initially be developed and tested for the OH opco.



s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/
s3://aep-datalake-raw-dev/util/intervals/nonvee/uiq_info/oh/20260219_2001/
