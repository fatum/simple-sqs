require "bundler/gem_tasks"

require 'simple/sqs'
require 'simple/sqs/worker'

Aws.config[:region] = 'eu-west-2'
Aws.config[:credentials] = Aws::Credentials.new(
  'KEY',
  'SECRET'
)

task :default do
  worker = Simple::Sqs::Worker.new(
    queue: "development_default",
    worker_size: 2
  )

  worker.execute do |message|
    p message.body

    sleep 1
  end
end
