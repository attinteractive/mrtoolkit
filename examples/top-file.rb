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

    emit :path
    emit :count
  end
  def process(input, output)
    if input.request =~ /GET\s+(\S+)\s/
      output.path = $1
      output.count = 1
      return output
    end
    nil
  end
end

class MainJob < JobBase
  def job
    mapper MainMap
    reducer MaxUniqueSumReduce, 10
    indir "logs"
    outdir "top-file"
  end
end
