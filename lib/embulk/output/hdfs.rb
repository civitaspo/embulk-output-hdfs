Embulk::JavaPlugin.register_output(
  "hdfs", "org.embulk.output.HdfsOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
