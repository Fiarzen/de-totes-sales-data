variable "ingestion_bucket_prefix" {
  type = string
  default = "ingestion-bucket"
}

variable "processing_bucket_prefix" {
  type = string
  default = "processing-bucket"
}

variable "load_bucket_prefix" {
  type = string
  default = "load-bucket"
}

variable "extract_lambda" {
  type = string
  default = "extract_lambda"
}

variable "transform_lambda" {
  type = string
  default = "transform_lambda"
}

variable "load_lambda" {
  type = string
  default = "load_lambda"
}

variable "default_timeout" {
  type    = number
  default = 60
}

variable "load_timeout" {
  type    = number
  default = 360
}

variable "region" {
  type = string  
}

variable "profile" {
  type = string
}