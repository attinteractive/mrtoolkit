
# StreamRunner
# This class is responsible for running stream jobs in hadoop.
#
# Streaming is a simplified programming model in which map and reduce
# proceses that read from STDIN and write to STDOUT are given.  
# StreamRunner runs *ruby programs* as the map and reduce steps.
# 
# Additional services provided:
# * the number of reducers can be specified
# * extra files to include can be given
# * input can be one directory or an array of directories
# * collects the output and copies it to local file in "out" directory
# * deletes hadoop output direcory before starting job -- BE CAREFUL
#
# Extra files are distributed to each cluster member, and are stored in the
# directory with the map or reduce programs.  You must include any data files 
# your program reads or library files it requires.  You do not have to
# include the program itself -- this is done automatically.
#
# HADOOP_HOME must be set.
# It might be necessary to change HADOOP_STREAMING_VERSION if the version changes.

HADOOP_STREAMING_VERSION="0.20.0"

HADOOP_HOME=ENV['HADOOP_HOME']
HADOOP_STREAMING="#{HADOOP_HOME}/contrib/streaming/hadoop-#{HADOOP_STREAMING_VERSION}-streaming.jar"

class StreamRunner
  def expand_path(file)
    return file if File.exist?(file)
    rlib = ENV['RUBYLIB']
    raise "Cannot resolve path to #{file} -- no RUBYLIB" unless rlib
    rlib.split(':').each do |rp|
      trial = "#{rp}/#{file}"
      return trial if File.exists?(trial)
    end
    raise "Cannot resolve path to #{file}"
  end
  def expand_paths(extra)
    extras = []
    extra.collect { |e| expand_path(e)}
  end

  def run_hadoop_stream(input, out, mapper, reducer, reducers, extra, 
    map_opts, reduce_opts, opts)
    extras = ''
    extra << mapper.split(' ')[0]
    extra << reducer.split(' ')[0]
    expand_paths(extra.uniq).each {|e| extras += "-file #{e} "}
    map_opt = ''
    map_opts.each {|n, v| map_opt += "-jobconf #{n}=#{v} "}
    reduce_opt = ''
    reduce_opts.each {|n, v| reduce_opt += "-jobconf #{n}=#{v} "}
    if input.class == Array
      input = input.collect {|i| "-input #{i}"}
    else
      input = "-input #{input}"
    end

    if reducer.nil?
      cmd = "hadoop jar #{HADOOP_STREAMING} " +
        "#{input} " +
	"-output NONE " +
	"-mapper \"ruby #{mapper}\"" +
        "-jobconf mapred.reduce.tasks=0 " +
	map_opt +
        "#{extras}"
    else
      cmd = "hadoop jar #{HADOOP_STREAMING} " +
        "#{input} " +
	"-output #{out} " +
	"-mapper \"ruby #{mapper}\" " +
	map_opt +
	"-reducer \"ruby #{reducer}\" " +
        "-jobconf mapred.reduce.tasks=#{reducers} " +
	reduce_opt +
        "#{extras}"
    end
    puts cmd if opts.has_key?(:verbose)
    system(cmd)
  end

  def run_map_reduce(input, out, map, reduce, reducers, extra, 
    map_opts = {}, reduce_opts = {}, opts = {})
    system("hadoop fs -rmr #{out}")
    system("rm -rf out/#{out}")
    system("mkdir -p out/#{out}")
    run_hadoop_stream(input, out, map, reduce, reducers, extra, 
      map_opts, reduce_opts, opts)
    (0..reducers-1).each do |i|
      n = sprintf("%05d", i)
      system("hadoop fs -cat #{out}/part-#{n} >out/#{out}/part-#{n}")
    end
  end
end

