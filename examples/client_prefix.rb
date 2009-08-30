require 'rubygems'
#require 'gearman'
require '../lib/gearman'
Gearman::Util.debug = true

servers = ['localhost:4730', 'localhost:4731']

ability_name_with_prefix = Gearman::Util.ability_name_with_prefix("test","sleep")

client = Gearman::Client.new(servers)

task = Gearman::Task.new(ability_name_with_prefix, 20)
task.on_complete {|d| puts d }

client.run task
