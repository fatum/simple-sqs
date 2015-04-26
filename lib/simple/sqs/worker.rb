require 'thread/pool'
require 'thread/channel'

require 'aws-sdk'

Thread.abort_on_exception = true

module Simple
  module Sqs
    class Worker
      def initialize(options)
        @options = {poller_size: 1}.merge(options)
      end

      def execute(&block)
        pool = Thread.pool 1, @options[:worker_size]

        subscribe_to_messages(pool, &block)

        %w(SIGTERM INT).each do |signal|
          trap signal do
            p "Receive #{signal} signal. Shutting down pool..."

            Thread.new do
              @shutdown = true

              pool.shutdown
              (@poller_threads || []).map(&:exit)

              p "All done. Exit..."
              Process.exit(0)
            end
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
              break if @shutdown

              queue.receive_message(limit: 10).
                each do |message|
                  break if @shutdown

                  pool.process do
                    begin
                      unless @shutdown
                        yield message
                        message.delete
                      end
                    rescue StandardError => e
                     p e.message
                     raise e
                   end
                  end
                end

              sleep 0.1
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
