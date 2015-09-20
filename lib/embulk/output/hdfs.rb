Embulk::JavaPlugin.register_output(
  "hdfs", "org.embulk.output.hdfs.HdfsFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
