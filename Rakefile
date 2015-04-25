require "bundler/gem_tasks"

require 'simple/sqs'
require 'simple/sqs/worker'

task :default do
  worker = Simple::Sqs::Worker.new(
    queue: "queue",
    worker_size: 1
  )

  worker.execute do |message|
    p message.body

    sleep 1
  end
end
