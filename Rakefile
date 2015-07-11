require "bundler/gem_tasks"

require 'simple/sqs'
require 'simple/sqs/worker'

Aws.config[:region] = 'us-west-2'
Aws.config[:credentials] = Aws::Credentials.new(
  'AKIAJ7NYXYRTMBLJDXXA',
  'seg6igHzT08bYO6TNcQxP2ws7wH4bobbBUDVMUwC'
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
