require 'rubygems'
require '../lib/gearman'

Gearman::Util.debug = true

servers = ['localhost:4730']
worker = Gearman::Worker.new(servers)

worker.add_ability('chunked_transfer') do |data, job|
  5.times do |i|
    sleep 1
    job.send_partial("CHUNK #{i}")
  end
  "EOD"
end
worker.work
