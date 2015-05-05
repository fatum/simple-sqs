require 'thread/pool'
require 'thread/channel'

require 'aws-sdk'

Thread.abort_on_exception = true

module Simple
  module Sqs
    class Worker
      def initialize(options)
        @options = {
          limit: 10,
          wait: false,
          poller_size: 1,
          idle_timeout: 20,
          wait_time_seconds: 20,
        }.merge(options)

        @pool = Thread.pool 1, @options[:worker_size]
      end

      def execute(&block)
        subscribe_to_messages(&block)
        trap_signals

        loop do
          sleep 1
        end
      end

      private

      def subscribe_to_messages(&block)
        queue = AWS::SQS.new.queues.named(@options[:queue])

        @poller_threads = @options[:poller_size].times.map do
          Thread.new do
            loop do
              break if @shutdown

              handle(
                queue.receive_message(
                  limit: @options[:limit],
                  wait_time_seconds: @options[:idle_timeout]
                ),
                &block)
            end
          end
        end
      end

      def handle(messages, &block)
        if messages.any?
          messages.each do |message|
            break if @shutdown

            @pool.process do
              begin
                unless @shutdown
                  block[message]

                  message.delete
                end
              rescue StandardError => e
               p e.message
               raise e
             end
            end
          end

          @pool.wait if @options[:wait]
        else
          sleep @options[:idle_timeout]
        end
      end

      def trap_signals
        trap 'TTIN' do
          puts "   Thread count: #{Thread.list.count}"

          puts "   Thread inspections:"
          Thread.list.each do |thread|
            puts "    #{thread.object_id}: #{thread.status}"
          end

          puts "   GC stats:"
          puts GC.stat

          puts "   Object Space:"

          counts = Hash.new{ 0 }
          ObjectSpace.each_object do |o|
            counts[o.class] += 1
          end

          puts counts
        end

        %w(SIGTERM INT).each do |signal|
          trap signal do
            puts "Receive #{signal} signal. Shutting down pool..."

            Thread.new do
              @shutdown = true

              @pool.shutdown
              (@poller_threads || []).map(&:exit)

              puts "All done. Exit..."
              Process.exit(0)
            end
          end
        end
      end
    end
  end
end
