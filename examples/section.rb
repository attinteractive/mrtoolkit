require 'mrtoolkit'

class MainMap < MapBase
  def declare
    # declare log fields
    field :ip
    field :client_id
    field :user_id
    field :dt_tm
    field :request
    field :status
    field :result_size
    field :referer
    field :ua

    emit :section
    emit :count
  end
  def process(input, output)
    if input.request =~ /\/(\w+)\//
      output.section = $1
      output.count = 1
      return output
    end
    nil
  end
end

class MainJob < JobBase
  def job
    mapper MainMap
    reducer UniqueSumReduce
    indir "logs"
    outdir "section"
  end
end

