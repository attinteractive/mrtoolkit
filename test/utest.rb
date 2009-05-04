require 'test/unit'
require 'mrtoolkit'
require 'regression'

##############################
# Log example.
#
# Reformats the date and time into one field.
# Reducer adds an extra column

class LogMap < MapBase
  def declare
    field :date
    field :time
    field :url
  
    emit :date_time
    emit :url
  end

  def process(input, output)
    output.date_time = input.date + "T" + input.time
    output.url = input.url
    output
  end
  
end

class LogReduce < ReduceBase
  def declare
    field :date_time
    field :url

    emit :date_time
    emit :url
    emit :junk
  end

  def process(input, output)
    output = copy_struct input, output
    output.junk = "x"
    output
  end
end

class LogJob < JobBase
  def job
    mapper LogMap
    reducer LogReduce
    infiles "test-in/test1-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_log
    LogJob.run_test_command
    out = File.read("test-out")
    expected = "2008-10-01T10:30:00\t1.2.3.4\tx\n" + 
               "2008-10-02T11:30:00\t1.2.3.5\tx\n"
    assert_equal(expected, out)
  end
end


##########################################
#
# Computs count, total, and sum of squares.

class SumMap < MapBase
  def declare
    field :value
  
    emit :count
    emit :total
    emit :sum_of_squares
  end

  def process(input, output)
    v = input.value.to_f
    output.count = 1
    output.total = v
    output.sum_of_squares = v * v
    output
  end
  
end

# This could be done with canned reducer
class MySumReduce < ReduceBase
  def declare
    field :count
    field :total
    field :sum_of_squares

    emit :count
    emit :total
    emit :sum_of_squares
  end

  def process_begin(dummy, output)
    @count = 0
    @total = 0
    @sum_of_squares = 0
    nil
  end
  def process(input, output)
    @count += input.count.to_f
    @total += input.total.to_f
    @sum_of_squares += input.sum_of_squares.to_f
    nil
  end
  def process_end(dummy, output)
    output.count = @count
    output.total = @total
    output.sum_of_squares = @sum_of_squares
    output
  end
end

class SumJob < JobBase
  def job
    mapper SumMap
    reducer MySumReduce
    infiles "test-in/test2-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_sum
    SumJob.run_test_command
    out = File.read("test-out")
    expected = "4.0\t43.0\t1005.0\n"
    assert_equal(expected, out)
  end
end


######################################
#
# Grops times into one-minute buckets
# Calculates counts for each bucket

require 'parsedate'

class MinMap < MapBase
  def declare
    field :dt
    field :tm
  
    emit :minute
    emit :count
  end

  def process(input, output)
    res = ParseDate.parsedate(input.dt + " " + input.tm)
    t = Time.local(*res)
    min = t.min + 60 * (t.hour + 24 * t.wday)
    output.count = 1
    output.minute = min
    output
  end
  
end

class MyMinReduce < ReduceBase
  def declare
    field :minute
    field :count

    emit :min
    emit :count
  end

  def process_init(input, output)
    @count = 0
    nil
  end
  def process_each(input, output)
    @count += 1
    nil
  end
  def process_term(input, output)
    output.min = @last
    output.count = @count
    output
  end
end

class MyMinJob < JobBase
  def job
    mapper MinMap
    reducer MyMinReduce
    infiles "test-in/test3-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_min
    MyMinJob.run_test_command
    out = File.read("test-out")
    expected = "8460\t1\n" +
               "8461\t1\n" +
               "8470\t3\n"
    assert_equal(expected, out)
  end
end

#################################
# 
# This is the previous one, but with a standard reducer.

class CollectJob < JobBase
  def job
    mapper MinMap
    reducer CopyReduce, 1
    infiles "test-in/test3-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_collect
    CollectJob.run_test_command
    out = File.read("test-out")
    expected = "8460\n" +
               "8461\n" +
               "8470\n" +
               "8470\n" +
               "8470\n"
    assert_equal(expected, out)
  end
end

#################################
# 
# This is the previous one, but with adifferent
# standard reducer.  This produces the same output
# as the custom reducer.

class UniqueJob < JobBase
  def job
    mapper MinMap
    reducer UniqueReduce
    infiles "test-in/test3-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_unique
    UniqueJob.run_test_command
    out = File.read("test-out")
    expected = "8460\n" +
               "8461\n" +
               "8470\n"
    assert_equal(expected, out)
  end
end

###############################
#
# Exercises SumReduce, which sums a variable
# set of columns.


class GSumJob < JobBase
  def job
    mapper CopyMap, 3
    reducer SumReduce, 3
    infiles "test-in/test6-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_gsum
    GSumJob.run_test_command
    out = File.read("test-out")
    expected = "12.0\t9.0\t8.0\n"
    assert_equal(expected, out)
  end
end

class SelectJob < JobBase
  def job
    mapper SelectMap, /^10[23]/
    reducer CopyReduce
    infiles "test-in/test5-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_select
    SelectJob.run_test_command
    out = File.read("test-out")
    expected = "102\n102\n102\n102\n103\n"
    assert_equal(expected, out)
  end
end

class SampleJob < JobBase
  def job
    mapper CopyMap, 3
    reducer SampleReduce, 10
    infiles "test-in/test7-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_sample
    srand 1234
    SampleJob.run_test_command
    out = File.read("test-out")
    expected = "5\n20\n7\n12\n2\n8\n3\n16\n17\n18\n"
    assert_equal(expected, out)
  end
end

class MaxJob < JobBase
  def job
    mapper CopyMap, 3
    reducer MaxReduce, 3
    infiles "test-in/test4-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_max
    MaxJob.run_test_command
    out = File.read("test-out")
    expected = "4\t10\n3\t3\n2\t2\n"
    assert_equal(expected, out)
  end
end

class MinJob < JobBase
  def job
    mapper CopyMap, 3
    reducer MinReduce, 3
    infiles "test-in/test4-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_min
    MinJob.run_test_command
    out = File.read("test-out")
    expected = "3\t3\n2\t2\n1\t1\n"
    assert_equal(expected, out)
  end
end

class UniqueSumJob < JobBase
  def job
    mapper CopyMap, 2
    reducer UniqueSumReduce
    infiles "test-in/test5-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_unique_sum
    UniqueSumJob.run_test_command
    out = File.read("test-out")
    expected = "100\t3\n101\t2\n102\t4\n103\t1\n104\t2\n"
    assert_equal(expected, out)
  end
end

class UniqueCountJob < JobBase
  def job
    mapper CopyMap
    reducer UniqueCountReduce
    infiles "test-in/test5-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_unique_count
    UniqueCountJob.run_test_command
    out = File.read("test-out")
    expected = "100\t3\n101\t2\n102\t4\n103\t1\n104\t2\n"
    assert_equal(expected, out)
  end
end

class MaxUniqueSumJob < JobBase
  def job
    mapper CopyMap, 3
    reducer MaxUniqueSumReduce, 3
    infiles "test-in/test5-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_max_unique_sum
    MaxUniqueSumJob.run_test_command
    out = File.read("test-out")
    expected = "102\t4\n100\t3\n101\t2\n"
    assert_equal(expected, out)
  end
end

class UniqueIndexedSumJob < JobBase
  def job
    mapper CopyMap, 3
    reducer UniqueIndexedSumReduce, 3
    infiles "test-in/test8-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_unique_indexed_sum
    UniqueIndexedSumJob.run_test_command
    out = File.read("test-out")
    expected = "100\t1000\t3\n100\t1001\t1\n200\t1000\t2\n200\t1001\t1\n"
    assert_equal(expected, out)
  end
end

class UniqueFirstJob < JobBase
  def job
    mapper CopyMap, 4
    reducer UniqueFirstReduce, 3, 1
    infiles "test-in/test9-in"
    outfiles "test-out"
  end
end

class TestMRToolkit < Test::Unit::TestCase
  def test_unique_first
    UniqueFirstJob.run_test_command
    out = File.read("test-out")
    expected = "a\ta\ta\nx1\ty1\tz1\n"
    assert_equal(expected, out)
  end
end


class TestRegression < Test::Unit::TestCase
  def test_regress
    x = [1, 2, 3]
    y = [1, 2, 3]
    reg = LinearRegression.new(x, y)
    assert_equal([1, 2, 3], reg.fit(x))
    x = [1, 2, 3, 4]
    y = [1, 5, 5, 9]
    reg = LinearRegression.new(x, y)
    assert_equal(2, reg.slope)
    assert_equal(0, reg.offset)
    y = [1, 5, 5, 9]
    reg = LinearRegression.new(x, y)
    assert_equal(2, reg.slope)
    assert_equal(0, reg.offset)
  end
end


