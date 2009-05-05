require 'pp'
require 'stream_runner'

# Store information about a processing stage.
# Includes input and output field names, field separators, 
# and the filenames processed by the stage.
class Stage
  attr_reader :in_fields, :out_fields
  attr_reader :in_sep, :out_sep
  attr_reader :errors

  def initialize(*args)
  end

  def field name
    @in_fields = [] unless @in_fields
    @in_fields << name.to_sym
  end
  def emit name
    @out_fields = [] unless @out_fields
    @out_fields << name.to_sym
  end
  def field_separator sep
    @in_sep = sep
  end
  def emit_separator sep
    @out_sep = sep
  end
  def catch_errors
    @catch_errors = true
  end

  def declare
  end

  # Create the input and output structures.
  def prepare
    @in_sep = "\t" unless @in_sep
    @out_sep = "\t" unless @out_sep
    @input_type = Struct.new(*@in_fields)
    @output_type = Struct.new(*@out_fields)
    @errors = 0
  end

  # Copies all fields of a struct to another
  # Some fields can be skipped.
  def copy_struct(src, dest, skip = 0)
    (0..src.length-1-skip).each {|i| dest[i] = src[i+skip]}
    dest
  end

  # Write any output
  def write_out(output)
    if output
      outs = @out_fields.collect { |f| output[f].to_s.chomp }
      @out_fd.puts outs.join(@out_sep)
    end
  end

  def new_input(line = nil)
    input = @input_type.new
    return input unless line
    fields = line.chomp.split(@in_sep)
    @in_fields.each_index { |i| input[i] = fields[i] }
    input
  end
  def new_output
    @output_type.new
  end

  # Process one line of map or reduce file.
  # Create output record.
  # Call the given function.
  # collect the output and write it out.
  def process_step(fun, input = nil)
    begin
      out = send(fun, input, new_output)
      if out
        out = [out] unless out.class == Array
        out.each {|o| write_out(o)}
      end
    rescue StandardError
      STDERR.puts "Error: #{$!}"
      @errors += 1
      raise unless @catch_errors
    end
  end
end

# This class allows uniform processing of File and STDIN/STDOUT
# file descriptors.
# It must be passed a block, which gets the open file descriptor.
class StreamIO
  def self.open(f, mode = "r")
    if f.class == String
      fp = File.open(f, mode)
      yield(fp)
      fp.close
    elsif f.class == IO
      yield(f)
    end
  end
end

############################
# Base class for map
############################
# Map Stage
# Creates an object to hold input lines after they have been
# parsed and separated into fields.
# Reads input and feeds to process method, then collects output.
class MapBase < Stage

  # Called at the beginning of map.
  # No input.
  def process_begin(dummy, output)
    nil
  end
  # Called for each record.
  def process(input, output)
    nil
  end
  # Called at the end of map.
  def process_end(dummy, output)
    nil
  end

  def run(in_fd, out_fd)
    @out_fd = out_fd
    process_step(:process_begin, nil)
    input = nil
    in_fd.each_line do |line|
      @raw_input = line
      input = new_input(line)
      process_step(:process, input)
    end
    process_step(:process_end, nil)
  end
end

############################
# Base class for reduce
############################
# Reduce Stage
# Creates an object to hold input lines after they have been
# parsed and separated into fields.
# Reads input and feeds to process method, then collects output.
# Reduce input is map output
class ReduceBase < Stage
  # This suite of functions is called on the fields based on
  # their first field.  
  # For each value of the first field, process_init is called first,
  # then process_each is called for each one, 
  # then process_term is called after the last one.  
  # The client can implement only process_term to see each unique value once.
  # When process_term is called, input is on the next record, so the first
  # field is in @last
  
  # Called at the beginning of a run of equal values of the first field.
  def process_init(input, output)
    nil
  end
  # Called for each one of the equal values.
  def process_each(input, output)
    nil
  end
  # Called after the run of equal values.
  # No input record. Previous value of first field in @last.
  def process_term(dummy, output)
    nil
  end

  # Called at the beginning of reduction.
  # No input.
  def process_begin(dummy, output)
    nil
  end
  # Called for each record.
  def process(input, output)
    nil
  end
  # Called at the end of reduction.
  def process_end(dummy, output)
    nil
  end

  # This suite of functions is called on all records.
  # The function process_begin is called first, 
  # then process is called on each record, 
  # then process_end is called last.
  # This default implementation implements the calls to process_init,
  # proces_each, and process_term.
  # The client can omit process_begin and process_end 
  # and just implement process to see each record.
  def process_internal(input)
    v = input[0]
    if @last.nil?
      process_step(:process_init, input)
      process_step(:process_each, input)
      @last = v
      return
    end
    if v == @last
      # As long as key is the same, just process it
      process_step(:process_each, input)
      return
    end
    # The run has ended
    process_step(:process_term, input) if @last
    @last = v

    process_step(:process_init, input)
    process_step(:process_each, input)
  end
  def process_end_internal(dummy)
    process_step(:process_term, nil) if @last
  end  

  # Run the reducer.
  # Call process_begin, then for each line, call
  # process, then call process_end.
  # At each step, collect any output and write it out.
  def run(in_fd, out_fd)
    @out_fd = out_fd
    @last = nil
    process_step(:process_begin, nil)

    input = nil			# so it will survive the loop
    in_fd.each_line do |line|
      @raw_input = line
      input = new_input(line)
      process_internal(input)
      process_step(:process, input)
    end
    process_end_internal(nil)
    process_step(:process_end, nil)
  end
end

#########################################
# Pre-written map and reduce classes
#########################################

# Map just copies its fields
class CopyMap < MapBase
  def initialize(*args)
    if args.size < 1
      @n = 0
    else
      @n = args[0].to_i - 1
    end
  end
  def declare
    (0..@n).each {|i| field "col#{i}"}

    (0..@n).each {|i| emit "col#{i}"}
  end

  def process(input, output)
    copy_struct(input, output)
  end
end

# Map selects according to a RE
class SelectMap < MapBase
  def initialize(*args)
    raise ArgumentError if args.size < 1
    @re = args[0]
    if args[1]
      @field = args[1]
    else
      @field = 0
    end
    if args[2]
      @n = args[2].to_i - 1
    else
      @n = 0
    end
  end
  def declare
    (0..@n).each {|i| field "col#{i}"}

    (0..@n).each {|i| emit "col#{i}"}
  end

  def process(input, output)
    if input[@field] =~ @re
      return copy_struct(input, output)
    end
    nil
  end
end

# Reducer collects all values
# Outputs as many lines as input
# Init with number of fields to copy (default 1).
# Optional second arg is the number of initial fields to skip.
class CopyReduce < ReduceBase
  def initialize(*args)
    if args[0]
      @n = args[0].to_i - 1
    else
      @n = 0
    end
    if args[1]
      @m = args[1].to_i - 1
    else
      @m = -1
    end
  end
  def declare
    (0..@m).each {|i| field "skip#{i}"}
    (0..@n).each {|i| field "col#{i}"}

    (0..@n).each {|i| emit "col#{i}"}
  end

  def process(input, output)
    copy_struct(input, output, @m+1)
  end
end

# Reducer collects unique values 
# Outputs as many lines as there are unique values in the first field.
class UniqueReduce < ReduceBase
  def declare
    field :value

    emit :value
  end

  def process_term(input, output)
    output.value = @last
    output
  end
end

# Reducer sums given fields
# Specify how many fields to sum (default 1).
# May optionally specify how many initial fields to skip
# Outputs one line of sums
class SumReduce < ReduceBase
  def initialize(*args)
    if args[0]
      @n = args[0].to_i - 1
    else
      @n = 0
    end
    if args[1]
      @m = args[1].to_i - 1
    else
      @m = -1
    end
  end
  def declare
    (0..@m).each {|i| field "skip#{i}"}
    (0..@n).each {|i| field "count#{i}"}

    (0..@n).each {|i| emit "sum#{i}"}
  end

  def process_begin(dummy, output)
    @sum = Array.new(@n+1, 0)
    nil
  end
  def process(input, output)
    (0..@n).each {|i| @sum[i] += input[i+@m+1].to_f}
    nil
  end
  def process_end(dummy, output)
    (0..@n).each {|i| output[i] = @sum[i]}
    output
  end
end

# This reducer sums within each unique value of the first field.
# Outputs one line of sums for each unique value of the first field.
class UniqueSumReduce < ReduceBase
  def initialize(*args)
    if args[0]
      @n = args[0].to_i - 1
    else
      @n = 0
    end
    if args[1]
      @m = args[1].to_i - 1
    else
      @m = -1
    end
  end

  def declare
    field :unique
    (0..@n).each {|i| field "count#{i}"}
    (0..@m).each {|i| field "extra#{i}"}

    emit :value
    (0..@n).each {|i| emit "sum#{i}"}
    (0..@m).each {|i| emit "extra#{i}"}
  end
  def process_init(input, output)
    @sum = Array.new(@n+1, 0)
    @extra = Array.new(@m+1)
    nil
  end
  def process_each(input, output)
    (0..@n).each {|i| @sum[i] += input[i+1].to_i}
    (0..@m).each {|i| @extra[i] = input[i+@n+2]}
    nil
  end
  def process_term(dummy, output)
    output.value = @last
    (0..@n).each {|i| output[i+1] = @sum[i]}
    (0..@m).each {|i| output[i+@n+2] = @extra[i]}
    output
  end
end

# Reducer counts within each unique value of the first field.
# Outputs one line of counts for each unique value of the first field.
class UniqueCountReduce < ReduceBase
  def initialize(*args)
    if args[0]
      @m = args[0].to_i - 1
    else
      @m = -1
    end
  end

  def declare
    field :unique
    (0..@m).each {|i| field "extra#{i}"}

    emit :value
    emit :count
    (0..@m).each {|i| emit "extra#{i}"}
  end
  def process_init(input, output)
    @count = 0
    @extra = Array.new(@m+1)
    nil
  end
  def process_each(input, output)
    @count += 1
    (0..@m).each {|i| @extra[i] = input[i+1]}
    nil
  end
  def process_term(dummy, output)
    output.value = @last
    output.count = @count
    (0..@m).each {|i| output[i+2] = @extra[i]}
    output
  end
end

# Reducer works on groups where the first field is the same.
# For each distinct value of the second field, sum up the values
#  of the third field.
class UniqueIndexedSumReduce < ReduceBase
  def declare
    field :unique
    field :index
    field :value

    emit :unique
    emit :index
    emit :value
  end
  def process_init(input, output)
    @sum = {}
    nil
  end
  def process_each(input, output)
    index = input.index
    @sum[index] = 0 unless @sum.has_key?(index) 
    @sum[index] += input.value.to_i 
    nil
  end
  def process_term(dummy, output)
    output = []
    @sum.each do |index, value|
      item = new_output
      item.unique = @last
      item.index = index
      item.value = value
      output << item
    end
    output
  end
end

# Reducer works on groups where the first field is the same.
# Count the number of distinct occurances of the second field.
class UniqueIndexedCountReduce < ReduceBase
  def declare
    field :unique
    field :index

    emit :unique
    emit :index
    emit :value
  end
  def process_init(input, output)
    @sum = {}
    nil
  end
  def process_each(input, output)
    index = input.index
    @sum[index] = 0 unless @sum.has_key?(index) 
    @sum[index] += 1
    nil
  end
  def process_term(dummy, output)
    output = []
    @sum.each do |index, value|
      item = new_output
      item.unique = @last
      item.index = index
      item.value = value
      output << item
    end
    output
  end
end

# Reducer samples the input
# One argument must be given: the number of samples to retain
# Outputs that many lines
# TODO store the whole input object in pool?
# or else take another argument of columns to store
class SampleReduce < ReduceBase
  def initialize(*args)
    raise ArgumentError if args.size < 1
    @m = args[0].to_i
  end

  def declare
    field :value

    emit :value
  end
  def process_begin(dummy, output)
    @pool = []
    @n = 0
    nil
  end
  def process(input, output)
    if @pool.size < @m
      @pool << input.value
    elsif rand < (@m.to_f / @n.to_f)
      @pool[rand(@m)] = input.value
    end  
    @n += 1
    nil
  end
  def process_end(dummy, output)
    output = []
    @pool.each do |elem|
      item = new_output
      item.value = elem
      output << item
    end
    output
  end
end

# Reducer retains the the M maximum values in column 2
# Column 2 must be numeric
# TODO store rest of fields too
class MaxReduce < ReduceBase
  def initialize(*args)
    if args[0]
      @m = args[0].to_i
    else
      @m = 1
    end
  end

  def declare
    field :key
    field :value

    emit :key
    emit :value
  end

  def compare(x, y)
    y <=> x
  end

  def sort_pool
    @pool.sort! {|x, y| compare(x[1], y[1])}
  end

  def process_begin(dummy, output)
    @pool = []
    nil
  end
  def process(input, output)
    val = input.value.to_i
    if @pool.size < @m
      @pool << [input.key, val]
      sort_pool
    elsif val > @pool[-1][1]
      @pool[-1] = [input.key, val]
      sort_pool
    end  
    nil
  end
  def process_end(dummy, output)
    output = []
    @pool.each do |elem|
      item = new_output
      item.key, item.value = elem
      output << item
    end
    output
  end
end

# Reducer sums the values for each unique key
# Outputs only the M max values
class MaxUniqueSumReduce < ReduceBase
  def initialize(*args)
    raise ArgumentError if args.size < 1
    @m = args[0].to_i
  end

  def declare
    field :key
    field :value

    emit :key
    emit :value
  end

  def sort_pool
    @pool.sort! {|x, y| y[1] <=> x[1]}
  end

  def process_begin(dummy, output)
    @pool = []
    nil
  end
  # These three do the sum
  def process_init(input, output)
    @sum = 0
    nil
  end
  def process_each(input, output)
    @sum += input.value.to_i
    nil
  end
  def process_term(dummy, output)
    if @pool.size < @m
      @pool << [@last, @sum]
      sort_pool
    elsif @sum > @pool[-1][1]
      @pool[-1] = [@last, @sum]
      sort_pool
    end  
    nil
  end
  def process_end(dummy, output)
    output = []
    @pool.each do |elem|
      item = new_output
      item.key, item.value = elem
      output << item
    end
    output
  end
end

# Min given fields
class MinReduce < MaxReduce
  def process(input, output)
    if @pool.size < @m
      @pool << [input.key, input.value]
      sort_pool
    elsif input.value.to_i < @pool[-1][1].to_i
      @pool[-1] = [input.key, input.value]
      sort_pool
    end  
    nil
  end
end

# First record of each unique field
# Drops the given number of colums.
# By default, drops the first column.
class UniqueFirstReduce < ReduceBase
  def initialize(*args)
    if args[0]
      @n = args[0].to_i - 1
    else
      @n = 0
    end
    if args[1]
      @m = args[1].to_i - 1
    else
      @m = -1
    end
  end

  def declare
    (0..@m).each {|i| field "skip#{i}"}
    (0..@n).each {|i| field "col#{i}"}

    (0..@n).each {|i| emit "col#{i}"}
  end
  # copy over all dest fields
  def process_init(input, output)
    copy_struct(input, output, @m+1)
  end
end

############################
# Base class for jobs
############################

class JobBase
  def initialize(*args)
    @stages = []
  end

  # Change filename so a path maps into a simple name.
  # /   ==> -
  # *   ==> all
  # []? ==> _
  def JobBase.filename_map(filename)
    filename.gsub(/\*/, 'all').gsub(/\//, '-').gsub(/[\[\]?]/, '_')
  end

  # These store job declarations
  def mapper map_class, *args
    @map_class = map_class
    @map_args = args
    @map_opts = {}
    @in_dirs = []
    @extras = []
  end
  def reducer reduce_class, *args
    @reduce_class = reduce_class
    @reduce_args = args
    @reducers = 1
    @reduce_opts = {}
  end
  def indir in_dir
    @in_dirs << in_dir
  end
  alias infiles indir
  def outdir out_dir
    @out_dir = JobBase.filename_map(out_dir)
  end
  alias outfiles outdir
  def reducers n
    @reducers = n
  end
  def extra ex
    @extras << ex
  end
  def map_opt n, v
    @map_opts[n] = v
  end
  def reduce_opt n, v
    @reduce_opts[n] = v
  end
  # This gathers the declarations and stores in a stage record.
  def add_stage
    case
    when @map_class.nil?: raise "Map class not specified"
    when @reduce_class.nil?: raise "Reduce class not specified"
    when @in_dirs.empty?: raise "Indir not speficied"
    when @out_dir.nil?: raise "Outdir not specified"
    end       
    @stages << [@map_class, @map_args, @map_opts,
      @reduce_class, @reduce_args, @reduce_opts, 
      @in_dirs, @out_dir, @reducers, @extras]
  end

  # For each method in the class starting with "stage", call the method,
  # then call add_stage.  This can be used to create multi-stage map-reduce 
  # programs.
  def prepare
    ms = self.class.instance_methods.find_all do |m|
      m =~ /(^stage)|(^job$)/
    end
    ms.sort.each do |m|
      self.method(m).call
      add_stage
    end
  end

  # Run the job.
  # For each stage, run the mapper, then sort the
  # intermediate output, then run the reducer.
  def run_test
    map_out_file = "/tmp/map-out"
    red_in_file = "/tmp/reduce-in"
    @stages.each do |s|
      map_class, map_args, map_opts, 
          reduce_class, reduce_args, reduce_opts,
          in_dirs, out_dir, reducers, extras = *s
      mapper = map_class.new(*map_args)
      mapper.declare
      mapper.prepare
      in_dirs.each do |in_dir|
        StreamIO.open(in_dir, "r") do |in_fd|
          StreamIO.open(map_out_file, "w") do |out_fd|
            mapper.run in_fd, out_fd
          end
        end
      end

      system "sort <#{map_out_file} >#{red_in_file}"

      reducer = reduce_class.new(*reduce_args)
      reducer.declare
      reducer.prepare
      StreamIO.open(red_in_file, "r") do |in_fd|
        StreamIO.open(out_dir, "w") do |out_fd|
          reducer.run in_fd, out_fd
        end
      end
    end
  end

  def self.run_test_command
    job = self.new
    job.prepare
    job.run_test
  end

  def build_command(fname, klass, args)
    res = "#{fname} -s #{klass.to_s}"
    if args
      res += " #{args.join(' ')}"
    end
    res
  end

  def self.get_job_opts
    opts = {}
    if ARGV[0] == '-v'
      opts[:verbose] = true
      ARGV.shift
    end
    opts
  end

  def run(fname, opts)
    sr = StreamRunner.new
    out_dir = "out"
    @stages.each do |s|
      map_class, map_args, map_opts, 
          reduce_class, reduce_args, reduce_opts,
          in_dirs, out_dir, reducers, extras = *s
      sr.run_map_reduce(in_dirs, out_dir, 
        build_command(fname, map_class, map_args),
        build_command(fname, reduce_class, reduce_args),
        reducers,
        [__FILE__, 'stream_runner.rb'] + extras, 
	map_opts, reduce_opts, opts)
    end
  end

  def self.run_command(filename = nil)
    filename = $0 unless filename    
    if ARGV[0] == '-s'
      ARGV.shift
      class_name = ARGV.shift
      action = Object.const_get(class_name).new(*ARGV)
      action.declare
      action.prepare
      action.run(STDIN, STDOUT)
    else
      opts = get_job_opts
      # create an instance of the class that was called originally
      action = self.new
      action.prepare
      action.run(File.basename(filename), opts)
    end
  end
end

# At exit, call run_command in each class of the form xxxJob.
at_exit do
  ObjectSpace.each_object(Class) do |klass|
    if klass.name =~ /^\w+Job$/
      klass.run_command
    end
  end
  exit 0
end
