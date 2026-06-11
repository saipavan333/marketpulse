output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.events.arn
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "warehouse_endpoint" {
  value     = aws_db_instance.warehouse.endpoint
  sensitive = true
}
