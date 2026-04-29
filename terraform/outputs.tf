output "s3_data_lake_bucket" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "redshift_endpoint" {
  description = "Redshift Serverless endpoint"
  value       = aws_redshiftserverless_workgroup.warehouse.endpoint
}

output "msk_bootstrap_brokers" {
  description = "MSK bootstrap broker string"
  value       = aws_msk_cluster.kafka.bootstrap_brokers
}

output "glue_catalog_databases" {
  description = "Glue catalog database names"
  value = {
    bronze = aws_glue_catalog_database.bronze.name
    silver = aws_glue_catalog_database.silver.name
    gold   = aws_glue_catalog_database.gold.name
  }
}
