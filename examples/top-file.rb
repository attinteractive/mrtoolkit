require 'mrtoolkit'

class TopFileMap < MapBase
  def declare
    # declare log fields
    field :ip
    field :client_id
    field :user_id
    field :dt_tm
    field :request
    field :status
    field :result_size
    field :referrer
    field :ua

    emit :path
    emit :count
  end
  def process(input, output)
    if input.request =~ /GET (\S*)\s/
      output.path = $1
      output.count = 1
    end
    output
  end
end

class MainJob < JobBase
  def stage1
    mapper TopFileMap
    reducer MaxUniqueSumReduce, 10
#    reducer UniqueSumReduce, 1
    indir "logs"
    outdir "top-file"
  end
end


MainJob.run_command(__FILE__)
