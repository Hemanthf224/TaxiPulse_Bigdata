package com.taxipulse

/**
 * Config.scala
 * Command line configuration parser for TaxiPulse Scala Preprocessing Job.
 */
case class PipelineConfig(
  inputPath: String = "hdfs://namenode:9000/taxipulse/raw",
  outputPath: String = "hdfs://namenode:9000/taxipulse/processed",
  referencePath: String = "hdfs://namenode:9000/taxipulse/reference",
  sample: Boolean = false,
  sampleRate: Double = 0.05
)

object Config {
  def parse(args: Array[String]): PipelineConfig = {
    var config = PipelineConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input" =>
          config = config.copy(inputPath = args(i + 1))
          i += 1
        case "--output" =>
          config = config.copy(outputPath = args(i + 1))
          i += 1
        case "--reference" =>
          config = config.copy(referencePath = args(i + 1))
          i += 1
        case "--sample" =>
          config = config.copy(sample = true)
        case "--sample-rate" =>
          config = config.copy(sampleRate = args(i + 1).toDouble)
          i += 1
        case _ =>
      }
      i += 1
    }
    config
  }
}
