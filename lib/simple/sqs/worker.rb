require 'thread/pool'
require 'thread/channel'
require 'thread_safe'

require 'aws-sdk'

Thread.abort_on_exception = true

module Simple
  module Sqs
    class Worker
      def initialize(options)
        @options = {poller_size: 1}.merge(options)
        @cache = ThreadSafe::Cache.new
      end

      def execute(&block)
        pool = Thread.pool 1, @options[:worker_size]

        subscribe_to_messages(pool, &block)

        trap "INT" do
          Thread.new do
            @cache[:shutdown] = true

            (@poller_threads || []).map(&:exit)

            pool.shutdown

            Process.exit(0)
          end
        end

        loop do
          sleep 1
        end
      end

      private

      def subscribe_to_messages(pool)
        @poller_threads = @options[:poller_size].times.map do
          Thread.new do
            loop do
              break if @cache[:shutdown]

              queue.receive_message(limit: 10).
                each do |message|
                  break if @cache[:shutdown]

                  pool.process do
                    begin
                      unless @cache[:shutdown]
                        yield message
                        message.delete
                      end
                    rescue StandardError => e
                     p e.message
                     raise e
                   end
                  end
                end
            end
          end
        end
      end

      def queue
        AWS::SQS.new.queues.named(@options[:queue])
      end
    end
  end
end
