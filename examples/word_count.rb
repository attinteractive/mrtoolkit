require 'rubygems'
require 'mrtoolkit'

class WordCountMap < MapBase
  def declare
    field :line

    emit :word
    emit :count
  end

  def process(input, output)

    if input.line
      tokens = input.line.downcase.gsub(/,|'|@|:|\/|\(|\)|-|\?|!|#|\./,'').chomp(".,\n\r").split(' ')

      words = tokens.select { |token| token =~ /^[a-z]+$/ }
      words.map do |word|
        output = new_output
        output.word = word.chomp.gsub(/\s+/, '')
        output.count = 1
        output
      end
    end
  end

end

class WordCountReduce < ReduceBase
  def declare
    field :word
    field :count

    emit :word
    emit :count
  end

  def process_init(input, output) 
    @total_count = 0
    @word = input.word
    nil
  end

  def process_each(input, output)
    @total_count += input.count.to_i
    nil
  end

  def process_term(dummy, output)
    output.word = @word
    output.count = @total_count
    output
  end
end

class WordCountJob < JobBase
  def job
    mapper WordCountMap
    reducer WordCountReduce
    indir "wc-indir"
    outdir "wc-outdir"
  end
end
